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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;


public class TarantoolClientImpl extends AbstractTarantoolOps<Integer, Object, Object, Future<List>> implements TarantoolClient, TarantoolConnectionOps<Integer, Object, Object, Future<List>> {

    /**
     * External
     */
    protected TarantoolClientConfig options;
    protected SocketChannelProvider socketProvider;
    /**
     * Connection state
     */
    protected SocketChannel channel;
    protected String salt;
    protected volatile Exception thumbstone;
    protected AtomicLong syncId = new AtomicLong();
    protected Map<Long, FutureImpl<List>> futures;

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
    protected ByteBuffer sharedBuffer;
    protected ReentrantLock bufferLock = new ReentrantLock(false);
    protected Condition bufferNotEmpty = bufferLock.newCondition();
    protected Condition bufferEmpty = bufferLock.newCondition();
    protected ReentrantLock writeLock = new ReentrantLock(true);

    /**
     * Interfaces
     */
    protected SyncOps syncOps = new SyncOps();
    protected FireAndForgetOps fireAndForgetOps = new FireAndForgetOps();


    /**
     * Inner
     */
    protected TarantoolClientStats stats;


    protected Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
            readThread();
        }
    });

    protected Thread writer = new Thread(new Runnable() {
        @Override
        public void run() {
            writeThread();
        }
    });



    protected  Thread connector = new Thread(new Runnable() {
        @Override
        public void run() {
            reconnect(0, thumbstone);
            LockSupport.park();
        }
    });

    protected void connect(SocketChannel channel) throws Exception {
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
            throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'", new IllegalStateException("Invalid welcome packet"));
        }
        is.readFully(bytes);
        this.salt = new String(bytes);
        if (options.username != null && options.password != null) {
            this.channel = channel;
            auth(options.username, options.password);
        }
        this.thumbstone = null;
        channel.socket().setSoTimeout(0);
        this.channel = channel;
        reader.setName("Tarantool " + channel.getRemoteAddress().toString() + " reader");
        writer.setName("Tarantool " + channel.getRemoteAddress().toString() + " writer");
        writer.setPriority(options.writerThreadPriority);
        reader.setPriority(options.readerThreadPriority);
        reader.start();
        writer.start();


    }


    public TarantoolClientImpl(SocketChannelProvider socketProvider, TarantoolClientConfig options) {
        this.thumbstone = new CommunicationException("Not connection, initializing connection");
        this.options = options;
        this.socketProvider = socketProvider;
        this.stats = new TarantoolClientStats();
        this.futures = new ConcurrentHashMap<Long, FutureImpl<List>>(options.predictedFutures);
        this.sharedBuffer = ByteBuffer.allocateDirect(options.sharedBufferSize);
        this.connector.setDaemon(true);
        this.connector.setName("Tarantool connector");
        reconnect(-1, null);
        return;
    }

    private void reconnect(int retry, Throwable lastError) {
        SocketChannel channel;
        while (!Thread.interrupted()) {
            channel = socketProvider.get(retry--, lastError);
            try {
                connect(channel);
                return;
            } catch (Exception e) {
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {

                    }

                }
                lastError = e;
            }
        }
    }

    @Override
    public FutureImpl<List> exec(Code code, Object... args) {
        FutureImpl<List> q = new FutureImpl<List>(syncId.incrementAndGet());
        if (isDead(q)) {
            return q;
        }
        futures.put(q.getId(), q);
        try {
            write(code, q.getId(), null, false, args);
        } catch (Exception e) {
            futures.remove(q.getId());
            q.setError(e);
        }
        return q;
    }

    protected synchronized void die(String message, Exception cause) {
        this.thumbstone = new CommunicationException(message, cause);
        while (!futures.isEmpty()) {
            Iterator<Map.Entry<Long, FutureImpl<List>>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, FutureImpl<List>> elem = iterator.next();
                if (elem != null) {
                    elem.getValue().setError(cause);
                }
                iterator.remove();
            }
        }
        close();
        if(connector.getState() == Thread.State.NEW) {
            connector.start();
        } else if (connector.getState() == Thread.State.WAITING) {
            LockSupport.unpark(connector);
        };

    }


    public void ping() {
        syncGet(exec(Code.PING));
    }


    protected void auth(String username, final String password) throws Exception {
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
        write(Code.AUTH, 0L, null, true, Key.USER_NAME, username, Key.TUPLE, auth);
        readPacket();
        long code = ((Long) headers.get(Key.CODE.getId()));
        if (code != 0) {
            throw createError(code, body.get(Key.ERROR.getId()));
        }
    }

    private void write(Code code, Long syncId, Long schemaId, boolean forceDirect, Object... args) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(options.defaultRequestSize + 5);
        bos.write(new byte[5]);
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
        ByteBuffer buffer = bos.toByteBuffer();
        buffer.put(0, (byte) 0xce);
        buffer.putInt(1, bos.size() - 5);


        if (sharedBuffer.capacity() * options.directWriteFactor <= buffer.limit() || forceDirect) {
            writeLock.lock();
            try {
                writeFully(channel, buffer);
                stats.directWrite++;
            } finally {
                writeLock.unlock();
            }
            return;
        }

        bufferLock.lock();
        try {
            while (sharedBuffer.remaining() < buffer.limit()) {
                try {
                    bufferEmpty.await();
                } catch (InterruptedException e) {
                    throw new CommunicationException("Interrupted", e);
                }
            }
            sharedBuffer.put(buffer);
            bufferNotEmpty.signalAll();
            stats.buffered++;
        } finally {
            bufferLock.unlock();
        }

    }


    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long written = 0;
        while (buffer.remaining() > 0 && (written = channel.write(buffer)) > -1) {
        }
        if (written < 0) {
            die("Can't write bytes", new SocketException("write failed"));
        }
    }


    protected void readThread() {
        while (!Thread.interrupted()) {
            try {
                long code;
                readPacket();
                code = (Long) headers.get(Key.CODE.getId());
                Long syncId = (Long) headers.get(Key.SYNC.getId());
                FutureImpl<List> future = futures.remove(syncId);
                stats.received++;
                complete(code, future);
            } catch (Exception e) {
                die("Cant read answer", e);
            }
        }
    }

    protected void readPacket() throws IOException {
        int size = ((Number) MsgPackLite.unpack(is, options.msgPackOptions)).intValue();
        long mark = bytesRead;
        is.mark(size);
        headers = (Map<Integer, Object>) MsgPackLite.unpack(is, options.msgPackOptions);
        if (bytesRead - mark < size) {
            body = (Map<Integer, Object>) MsgPackLite.unpack(is, options.msgPackOptions);
        }
        is.skipBytes((int) (bytesRead - mark - size));
    }


    protected void writeThread() {
        ByteBuffer local = ByteBuffer.allocateDirect(sharedBuffer.capacity());
        while (!Thread.interrupted()) {
            try {
                bufferLock.lock();
                if (sharedBuffer.position() == 0) {
                    bufferNotEmpty.await();
                }
                try {
                    sharedBuffer.flip();
                    local.put(sharedBuffer);
                    sharedBuffer.clear();
                    bufferEmpty.signalAll();
                } finally {
                    bufferLock.unlock();
                }
                local.flip();
                writeLock.lock();
                try {
                    writeFully(channel, local);
                } finally {
                    writeLock.unlock();
                }
                local.clear();
                stats.bufferedWrites++;
            } catch (IOException e) {
                die("Cant write bytes", e);
            } catch (InterruptedException e) {
                return;
            }
        }
    }


    protected void complete(long code, FutureImpl<List> q) {
        if (q != null) {
            if (code == 0) {
                q.setValue((List) body.get(Key.DATA.getId()));
            } else {
                Object error = body.get(Key.ERROR.getId());
                q.setError(createError(code, error));
            }
        }
    }

    protected TarantoolException createError(long code, Object error) {
        return new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
    }

    protected List syncGet(FutureImpl<List> r) {
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

    @Override
    public void close() {
        reader.interrupt();
        writer.interrupt();
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }

    @Override
    public TarantoolConnectionOps<Integer, Object, Object, List> syncOps() {
        return syncOps;
    }

    @Override
    public TarantoolConnectionOps<Integer, Object, Object, Future<List>> asyncOps() {
        return this;
    }

    @Override
    public TarantoolConnectionOps<Integer, Object, Object, Void> fireAndForgetOps() {
        return fireAndForgetOps;
    }

    protected class SyncOps extends AbstractTarantoolOps<Integer, Object, Object, List> implements TarantoolConnectionOps<Integer, Object, Object, List> {
        @Override
        public List exec(Code code, Object... args) {
            return syncGet(TarantoolClientImpl.this.exec(code, args));
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    protected class FireAndForgetOps extends AbstractTarantoolOps<Integer, Object, Object, Void> implements TarantoolConnectionOps<Integer, Object, Object, Void> {
        @Override
        public Void exec(Code code, Object... args) {
            try {
                if (thumbstone == null) {
                    write(code, syncId.incrementAndGet(), null, false, args);
                }
            } catch (Exception e) {

            }
            return null;
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    private boolean isDead(FutureImpl<List> q) {
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

    public TarantoolClientStats getStats() {
        return stats;
    }

    public TarantoolClientConfig getOptions() {
        return options;
    }
}
