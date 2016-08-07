package org.tarantool;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TarantoolClientImpl extends AbstractTarantoolConnection16<Integer, Object, Object, Future<List>> implements TarantoolClient, TarantoolConnection16Ops<Integer, Object, Object, Future<List>> {
    protected SocketChannel channel;
    protected String salt;
    protected volatile Exception thumbstone;

    protected AtomicLong syncId = new AtomicLong();
    protected Map<Long, AsyncQuery<List>> futures;

    protected int msgPackOptions = MsgPackLite.OPTION_UNPACK_NUMBER_AS_LONG | MsgPackLite.OPTION_UNPACK_RAW_AS_STRING;

    /**
     * Read properties
     */
    protected DataInputStream is;
    protected long bytesRead;
    protected Map<Integer, Object> headers;
    protected Map<Integer, Object> body;

    /**
     * Write properties
     */
    final Lock writeLock = new ReentrantLock();
    final Condition flushDone = writeLock.newCondition();
    protected ByteBuffer outBuffer;
    protected volatile long lastFlush;
    private long batchTimeout = 0L;

    /**
     * Interfaces
     */
    protected SyncOps syncOps = new SyncOps();
    protected FireAndForgetOps fireAndForgetOps = new FireAndForgetOps();

    protected Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
            readThread();
        }
    });

    protected Thread flusher = new Thread(new Runnable() {
        @Override
        public void run() {
            flushThread();
        }
    });


    public TarantoolClientImpl(SocketChannel channel, long batchTimeout, int predictedFutures) {
        try {
            this.channel = channel;
            this.futures = new ConcurrentHashMap<Long, AsyncQuery<List>>(predictedFutures);
            this.outBuffer = batchTimeout > 0 ? ByteBuffer.allocateDirect(channel.socket().getSendBufferSize()) : null;
            this.batchTimeout = batchTimeout;
            final InputStream inputStream = new BufferedInputStream(channel.socket().getInputStream(), channel.socket().getReceiveBufferSize());
            InputStream counter = new InputStream() {
                @Override
                public int read() throws IOException {
                    bytesRead++;
                    return inputStream.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int read = inputStream.read(b, off, len);
                    bytesRead += read;
                    return read;
                }
            };
            is = new DataInputStream(counter);
            byte[] bytes = new byte[64];
            is.readFully(bytes);
            String firstLine = new String(bytes);
            if (!firstLine.startsWith("Tarantool")) {
                channel.close();
                die("Welcome message should starts with tarantool but starts with '" + firstLine + "'", new IllegalStateException("Invalid welcome packet"));
            }
            is.readFully(bytes);
            this.salt = new String(bytes);
        } catch (IOException e) {
            die("Can't connect with tarantool", e);
        }
        try {
            channel.socket().setSoTimeout(0);
        } catch (SocketException e) {
            die("Couldn't disable read timeout", e);
        }
        reader.start();
        if (batchTimeout > 0) {
            flusher.start();
        }
    }

    @Override
    public AsyncQuery<List> exec(Code code, Object... args) {
        AsyncQuery<List> q = new AsyncQuery<List>(syncId.incrementAndGet(), code, args);
        if (isDead(q)) {
            return q;
        }
        futures.put(q.getId(), q);
        try {
            write(code, q.getId(), null, args);
        } catch (Exception e) {
            q.setError(e);
        }
        return q;
    }

    protected synchronized void die(String message, Exception cause) {
        this.thumbstone = cause;
        CommunicationException error = new CommunicationException(message, cause);
        while (!futures.isEmpty()) {
            Iterator<Map.Entry<Long, AsyncQuery<List>>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, AsyncQuery<List>> elem = iterator.next();
                if (elem != null) {
                    elem.getValue().setError(cause);
                }
                iterator.remove();
            }
        }
        close();
        throw error;
    }


    public void ping() {
        syncGet(exec(Code.PING));
    }


    public void auth(String username, final String password) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            List auth = new ArrayList(2);
            auth.add("chap-sha1");

            byte[] p = sha1.digest(password.getBytes());

            sha1.reset();
            byte[] p2 = sha1.digest(p);

            sha1.reset();
            sha1.update(Base64.decode(salt), 0, 20);
            sha1.update(p2);
            byte[] scramble = sha1.digest();
            for (int i = 0, e = 20; i < e; i++) {
                p[i] ^= scramble[i];
            }
            auth.add(p);
            syncGet(exec(Code.AUTH, Key.USER_NAME, username, Key.TUPLE, auth));

        } catch (NoSuchAlgorithmException e) {
            die("Can't use sha-1", e);
        }
    }

    private void write(Code code, Long syncId, Long schemaId, Object... args) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        DataOutputStream ds = new DataOutputStream(bos);
        Map<Key, Object> header = new EnumMap<Key, Object>(Key.class);
        Map<Key, Object> body = new EnumMap<Key, Object>(Key.class);
        header.put(Key.CODE, code);
        header.put(Key.SYNC, syncId);
        if (schemaId != null) {
            header.put(Key.SCHEMA_ID, schemaId);
        }
        if (args != null) {
            for (int i = 0, e = args.length; i < e; i += 2) {
                Object value = args[i + 1];
                body.put((Key) args[i], value);
            }
        }
        MsgPackLite.pack(header, ds);
        MsgPackLite.pack(body, ds);
        ds.flush();
        ByteBuffer sizeBuffer = ByteBuffer.allocate(5);
        sizeBuffer.put((byte) 0xce);
        sizeBuffer.putInt(bos.size()).position(5);
        sizeBuffer.flip();
        writeLock.lock();
        try {
            send(sizeBuffer);
            send(bos.toByteBuffer());
        } finally {
            writeLock.unlock();
        }
    }

    private void send(ByteBuffer buffer) throws IOException {
        if (outBuffer != null) {
            if (buffer.remaining() > outBuffer.remaining()) {
                flush();
                flushDone.signal();
                writeFully(buffer);
                lastFlush = System.currentTimeMillis();
            } else {
                outBuffer.put(buffer);
            }
        } else {
            writeFully(buffer);
        }

    }

    private void flush() throws IOException {
        if (outBuffer.position() > 0) {
            outBuffer.flip();
            writeFully(outBuffer);
            outBuffer.clear();
        }
    }

    private void writeFully(ByteBuffer buffer) throws IOException {
        int code = 0;
        while (buffer.remaining() > 0 && (code = channel.write(buffer)) > -1) {

        }
        if (code < 0) {
            die("Can't write bytes", new SocketException("write failed"));
        }
    }


    protected void readThread() {
        while (!Thread.interrupted()) {
            try {
                long code;
                int size = ((Number) MsgPackLite.unpack(is, msgPackOptions)).intValue();
                long mark = bytesRead;
                is.mark(size);
                headers = (Map<Integer, Object>) MsgPackLite.unpack(is, msgPackOptions);
                if (bytesRead - mark < size) {
                    body = (Map<Integer, Object>) MsgPackLite.unpack(is, msgPackOptions);
                }
                is.skipBytes((int) (bytesRead - mark - size));
                code = (Long) headers.get(Key.CODE.getId());
                Long syncId = (Long) headers.get(Key.SYNC.getId());
                AsyncQuery<List> future = futures.remove(syncId);
                complete(code, future);
            } catch (IOException e) {
                if (Thread.interrupted()) {
                    return;
                } else {
                    die("Cant read bytes", e);
                }
            }
        }
    }

    protected void flushThread() {
        while (!Thread.interrupted()) {
            try {
                writeLock.lock();
                try {
                    flushDone.await(batchTimeout, TimeUnit.MILLISECONDS);
                    if (System.currentTimeMillis() - lastFlush >= batchTimeout) {
                        try {
                            flush();
                            lastFlush = System.currentTimeMillis();
                        } catch (IOException e) {
                            die("Can't write bytes", e);
                        }
                    }
                } finally {
                    writeLock.unlock();
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    protected void complete(long code, AsyncQuery<List> q) {
        if (q != null) {
            if (code == 0) {
                q.setValue((List) body.get(Key.DATA.getId()));
            } else {
                Object error = body.get(Key.ERROR.getId());
                q.setError(new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error)));
            }
        }
    }

    protected List syncGet(AsyncQuery<List> r) {
        try {
            return r.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CommunicationException) {
                throw (CommunicationException) e.getCause();
            } else if (e.getCause() instanceof TarantoolException) {
                throw (TarantoolException) e.getCause();
            } else {
                throw new IllegalStateException(e.getCause());
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getMsgPackOptions() {
        return msgPackOptions;
    }

    public void setMsgPackOptions(int msgPackOptions) {
        this.msgPackOptions = msgPackOptions;
    }

    @Override
    public void close() {
        reader.interrupt();
        flusher.interrupt();
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }

    @Override
    public TarantoolConnection16Ops<Integer, Object, Object, List> syncOps() {
        return syncOps;
    }

    @Override
    public TarantoolConnection16Ops<Integer, Object, Object, Future<List>> asyncOps() {
        return this;
    }

    @Override
    public TarantoolConnection16Ops<Integer, Object, Object, Void> fireAndForgetOps() {
        return fireAndForgetOps;
    }

    protected class SyncOps extends AbstractTarantoolConnection16<Integer, Object, Object, List> implements TarantoolConnection16Ops<Integer, Object, Object, List> {
        @Override
        public List exec(Code code, Object... args) {
            return syncGet(TarantoolClientImpl.this.exec(code, args));
        }

        @Override
        public void auth(String username, String password) {
            TarantoolClientImpl.this.auth(username, password);
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    protected class FireAndForgetOps extends AbstractTarantoolConnection16<Integer, Object, Object, Void> implements TarantoolConnection16Ops<Integer, Object, Object, Void> {
        @Override
        public Void exec(Code code, Object... args) {
            try {
                if (thumbstone == null) {
                    write(code, syncId.incrementAndGet(), null, args);
                }
            } catch (Exception e) {

            }
            return null;
        }

        @Override
        public void auth(String username, String password) {
            TarantoolClientImpl.this.auth(username, password);
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    private boolean isDead(AsyncQuery<List> q) {
        if (TarantoolClientImpl.this.thumbstone != null) {
            q.setError(new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        return false;
    }


    protected class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        public ByteArrayOutputStream(int size) {
            super(size);
        }

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

    public Exception getThumbstone() {
        return thumbstone;
    }
}
