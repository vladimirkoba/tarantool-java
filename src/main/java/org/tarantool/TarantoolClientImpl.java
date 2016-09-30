package org.tarantool;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;


public class TarantoolClientImpl extends AbstractTarantoolOps<Integer, Object, Object, Future<List>> implements TarantoolClient, TarantoolConnectionOps<Integer, Object, Object, Future<List>> {

    /**
     * External
     */
    protected SocketChannelProvider socketProvider;
    /**
     * Connection state
     */
    protected SocketChannel channel;
    protected String salt;
    protected volatile Exception thumbstone;
    protected volatile CountDownLatch alive;

    protected AtomicLong syncId = new AtomicLong();
    protected Map<Long, FutureImpl<List>> futures;
    protected AtomicInteger wait = new AtomicInteger();

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
    protected ByteBuffer writerBuffer;
    protected ReentrantLock bufferLock = new ReentrantLock(false);
    protected Condition bufferNotEmpty = bufferLock.newCondition();
    protected Condition bufferEmpty = bufferLock.newCondition();
    protected ReentrantLock writeLock = new ReentrantLock(true);

    /**
     * Interfaces
     */
    protected SyncOps syncOps;
    protected FireAndForgetOps fireAndForgetOps;

    /**
     * Inner
     */
    protected TarantoolClientStats stats;
    protected Thread reader;
    protected Thread writer;


    protected Thread connector = new Thread(new Runnable() {
        @Override
        public void run() {
            reconnect(0, thumbstone);
            LockSupport.park();
        }
    });

    public TarantoolClientImpl(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        super(config);
        this.thumbstone = new CommunicationException("Not connection, initializing connection");
        this.alive = new CountDownLatch(1);
        this.socketProvider = socketProvider;
        this.stats = new TarantoolClientStats();
        this.futures = new ConcurrentHashMap<Long, FutureImpl<List>>(config.predictedFutures);
        this.sharedBuffer = ByteBuffer.allocateDirect(config.sharedBufferSize);
        this.writerBuffer = ByteBuffer.allocateDirect(sharedBuffer.capacity());
        this.connector.setDaemon(true);
        this.connector.setName("Tarantool connector");
        this.syncOps = new SyncOps(config);
        this.fireAndForgetOps = new FireAndForgetOps(config);
        reconnect(-1, null);
        return;
    }

    protected void reconnect(int retry, Throwable lastError) {
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

    protected void connect(final SocketChannel channel) throws Exception {
        is = new DataInputStream(new ByteBufferInputStream(channel));
        byte[] bytes = new byte[64];
        is.readFully(bytes);
        String firstLine = new String(bytes);
        if (!firstLine.startsWith("Tarantool")) {
            channel.close();
            throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'", new IllegalStateException("Invalid welcome packet"));
        }
        is.readFully(bytes);
        this.salt = new String(bytes);
        if (config.username != null && config.password != null) {
            this.channel = channel;
            auth(config.username, config.password);
        }
        this.thumbstone = null;
        alive.countDown();

        bufferLock.lock();
        try {
            sharedBuffer.clear();
        } finally {
            bufferLock.unlock();
        }
        final CountDownLatch init = new CountDownLatch(2);
        reader = new Thread(new Runnable() {
            @Override
            public void run() {
                init.countDown();
                readThread();
            }
        });
        writer = new Thread(new Runnable() {
            @Override
            public void run() {
                init.countDown();
                writeThread();
            }
        });

        channel.configureBlocking(false);
        this.channel = channel;

        reader.setName("Tarantool " + channel.getRemoteAddress().toString() + " reader");
        writer.setName("Tarantool " + channel.getRemoteAddress().toString() + " writer");
        writer.setPriority(config.writerThreadPriority);
        reader.setPriority(config.readerThreadPriority);
        reader.start();
        writer.start();
        init.await();
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
            throw serverError(code, body.get(Key.ERROR.getId()));
        }
    }


    @Override
    public FutureImpl<List> exec(Code code, Object... args) {
        FutureImpl<List> q = new FutureImpl<List>(syncId.incrementAndGet());
        if (isDead(q)) {
            return q;
        }
        futures.put(q.getId(), q);
        if (isDead(q)) {
            futures.remove(q.getId());
            return q;
        }
        try {
            write(code, q.getId(), null, false, args);
        } catch (Exception e) {
            futures.remove(q.getId());
            fail(q, e);
        }
        return q;
    }


    protected synchronized void die(String message, Exception cause) {
        if (thumbstone != null) {
            return;
        }
        this.thumbstone = new CommunicationException(message, cause);
        this.alive = new CountDownLatch(1);
        while (!futures.isEmpty()) {
            Iterator<Map.Entry<Long, FutureImpl<List>>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, FutureImpl<List>> elem = iterator.next();
                if (elem != null) {
                    FutureImpl<List> future = elem.getValue();
                    fail(future, cause);
                }
                iterator.remove();
            }
        }
        close();
        if (connector.getState() == Thread.State.NEW) {
            connector.start();
        } else if (connector.getState() == Thread.State.WAITING) {
            LockSupport.unpark(connector);
        }
    }


    public void ping() {
        syncGet(exec(Code.PING));
    }


    protected void write(Code code, Long syncId, Long schemaId, boolean forceDirect, Object... args)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(config.defaultRequestSize + 5);
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


        if (sharedBuffer.capacity() * config.directWriteFactor <= buffer.limit() || forceDirect) {
            writeLock.lock();
            try {
                writeFully(channel, buffer);
                stats.directWrite++;
                wait.incrementAndGet();
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
            wait.incrementAndGet();
            bufferNotEmpty.signalAll();
            stats.buffered++;
        } finally {
            bufferLock.unlock();
        }

    }


    protected void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long code = 0;
        while (buffer.remaining() > 0 && (code = channel.write(buffer)) > -1) {
        }
        if (code < 0) {
            throw new SocketException("write failed code: " + code);
        }
    }


    protected void readThread() {
        try {
            while (!Thread.interrupted()) {
                try {
                    long code;
                    readPacket();
                    code = (Long) headers.get(Key.CODE.getId());
                    Long syncId = (Long) headers.get(Key.SYNC.getId());
                    FutureImpl<List> future = futures.remove(syncId);
                    stats.received++;
                    wait.decrementAndGet();
                    complete(code, future);
                } catch (Exception e) {
                    die("Cant read answer", e);
                    return;
                }
            }
        } catch (Exception e) {
            die("Cant init thread", e);
        }
    }

    protected void readPacket() throws IOException {
        int size = ((Number) MsgPackLite.unpack(is, config.msgPackOptions)).intValue();
        long mark = bytesRead;
        is.mark(size);
        headers = (Map<Integer, Object>) MsgPackLite.unpack(is, config.msgPackOptions);
        if (bytesRead - mark < size) {
            body = (Map<Integer, Object>) MsgPackLite.unpack(is, config.msgPackOptions);
        }
        is.skipBytes((int) (bytesRead - mark - size));
    }


    protected void writeThread() {
        writerBuffer.clear();
        while (!Thread.interrupted()) {
            try {
                bufferLock.lock();
                if (sharedBuffer.position() == 0) {
                    bufferNotEmpty.await();
                }
                try {
                    sharedBuffer.flip();
                    writerBuffer.put(sharedBuffer);
                    sharedBuffer.clear();
                    bufferEmpty.signalAll();
                } finally {
                    bufferLock.unlock();
                }
                writerBuffer.flip();
                writeLock.lock();
                try {
                    writeFully(channel, writerBuffer);
                } finally {
                    writeLock.unlock();
                }
                writerBuffer.clear();
                stats.bufferedWrites++;
            } catch (Exception e) {
                die("Cant write bytes", e);
                return;
            }
        }
    }


    protected TarantoolException serverError(long code, Object error) {
        return new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
    }

    protected void fail(FutureImpl<List> q, Exception e) {
        q.setError(e);
    }

    protected void complete(long code, FutureImpl<List> q) {
        if (q != null) {
            if (code == 0) {
                q.setValue((List) body.get(Key.DATA.getId()));
            } else {
                Object error = body.get(Key.ERROR.getId());
                fail(q, serverError(code, error));
            }
        }
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
        if (reader != null) {
            reader.interrupt();
        }
        if (writer != null) {
            writer.interrupt();
        }
        if (is != null) {
            try {
                is.close();
            } catch (IOException ignored) {

            }
        }
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }

    @Override
    public boolean isAlive() {
        return thumbstone == null;
    }

    @Override
    public void waitAlive() throws InterruptedException {
        alive.await();
    }

    @Override
    public void waitAlive(long timeout, TimeUnit unit) throws InterruptedException {
        alive.await(timeout, unit);
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
    public TarantoolConnectionOps<Integer, Object, Object, Long> fireAndForgetOps() {
        return fireAndForgetOps;
    }

    protected class SyncOps extends AbstractTarantoolOps<Integer, Object, Object, List> implements TarantoolConnectionOps<Integer, Object, Object, List> {
        public SyncOps(TarantoolClientConfig config) {
            super(config);
        }

        @Override
        public List exec(Code code, Object... args) {
            return syncGet(TarantoolClientImpl.this.exec(code, args));
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    protected class FireAndForgetOps extends AbstractTarantoolOps<Integer, Object, Object, Long> implements TarantoolConnectionOps<Integer, Object, Object, Long> {
        public FireAndForgetOps(TarantoolClientConfig config) {
            super(config);
        }

        @Override
        public Long exec(Code code, Object... args) {
            if (thumbstone == null) {
                try {
                    long syncId = TarantoolClientImpl.this.syncId.incrementAndGet();
                    write(code, syncId, null, false, args);
                    return syncId;
                } catch (IOException e) {
                    throw new CommunicationException("Execute failed", e);
                }
            } else {
                throw new CommunicationException("Connection is not alive", thumbstone);
            }
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    protected boolean isDead(FutureImpl<List> q) {
        if (TarantoolClientImpl.this.thumbstone != null) {
            fail(q, new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        return false;
    }

    public Exception getThumbstone() {
        return thumbstone;
    }

    public TarantoolClientStats getStats() {
        return stats;
    }

    protected class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        public ByteArrayOutputStream(int size) {
            super(size);
        }

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

}
