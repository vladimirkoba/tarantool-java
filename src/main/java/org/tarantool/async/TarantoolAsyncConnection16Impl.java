package org.tarantool.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.tarantool.Code;
import org.tarantool.CommunicationException;
import org.tarantool.ConnectionState;
import org.tarantool.Key;
import org.tarantool.TarantoolConnection16Base;
import org.tarantool.TarantoolException;

public class TarantoolAsyncConnection16Impl extends TarantoolConnection16Base<Integer,Object,Object,Future<List>> implements TarantoolSelectorWorker.ChannelProcessor, TarantoolAsyncConnection16 {
    protected static final int ST_LENGTH = 0;
    protected static final int ST_BODY = 1;
    protected volatile SelectionKey key;
    protected AtomicLong syncId = new AtomicLong(0);
    protected ByteBuffer readBuffer;
    protected final ConnectionState writeState = new ConnectionState();
    protected ByteBuffer writeBuffer;
    protected LinkedBlockingQueue<AsyncQuery> writeQueue = new LinkedBlockingQueue<AsyncQuery>();
    protected Map<Long, AsyncQuery> futures = new ConcurrentHashMap<Long, AsyncQuery>();

    protected int conState = ST_LENGTH;
    protected boolean syncMode;
    protected volatile Exception error;
    protected volatile Long schemaId;


    public TarantoolAsyncConnection16Impl(TarantoolSelectorWorker worker, SocketChannel channel, String username, String password, long timeout, TimeUnit unit) {
        super(channel);
        syncMode = true;
        if (username != null) {
            this.auth(username, password);
        }
        syncMode = false;
        BlockingQueue<SelectionKey> queue = worker.register(channel, this);
        try {
            key = queue == null ? null : queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new CommunicationException("Can't register key", e);
        }
        if (key == null) {
            throw new CommunicationException("Can't register key");
        }
        readBuffer = in.getLengthReadBuffer();
    }

    @Override
    public void idle() {
        key.interestOps(SelectionKey.OP_READ | (writeQueue.isEmpty() ? 0 : SelectionKey.OP_WRITE));
    }

    @Override
    public void read() {
        try {
            int read = channel.read(readBuffer);
            if (read < 0) {
                close(new ClosedChannelException());
            }
            if (read > 0) {
                if (readBuffer.position() == readBuffer.limit()) {
                    if (conState == ST_LENGTH) {
                        readBuffer = in.getPacketReadBuffer();
                        conState = ST_BODY;
                        read();
                    } else if (conState == ST_BODY) {
                        in.unpack(msgPackOptions);
                        long code = (Long) in.getHeader().get(Key.CODE);
                        long syncId1 = (Long) in.getHeader().get(Key.SYNC);
                        schemaId = (Long) in.getHeader().get(Key.SCHEMA_ID);
                        AsyncQuery q = futures.remove(syncId1);
                        setFutureResult(code, q);
                        readBuffer = in.getLengthReadBuffer();
                        conState = ST_LENGTH;
                    }
                }
            }
        } catch (IOException e) {
            close(e);
        }
    }


    protected void setFutureResult(long code, AsyncQuery q) {
        if (q != null) {
            if (code != 0) {
                Object error = in.getBody().get(Key.ERROR);
                q.setError(new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error)));
            } else {
                q.setValue(in.getBody().get(Key.DATA));
            }

        }
    }

    @Override
    public void write() {
        if (writeBuffer == null) {
            AsyncQuery query = writeQueue.poll();
            try {
                if (query != null) {
                    writeBuffer = write(query);
                }
            } catch (Exception e) {
                query.setError(e);
            }
        }
        if (writeBuffer != null) {
            try {
                channel.write(writeBuffer);
                if (writeBuffer.remaining() == 0) {
                    writeBuffer = null;
                }
            } catch (IOException e) {
                close(e);
            }
        }
    }

    protected ByteBuffer write(AsyncQuery query) {
        return writeState.pack(query.code, query.id, query.args);
    }


    @Override
    public Future<List> exec(Code code, Object... args) {
        if (syncMode) {
            write(in.pack(code, args));
            AsyncQuery<List> q = new AsyncQuery<List>();
            q.setValue((List) readData());
            schemaId = (Long) in.getHeader().get(Key.SCHEMA_ID);
            return q;
        }
        if (key.isValid()) {
            AsyncQuery q = newAsyncQuery(syncId.incrementAndGet(), code, args);
            if (addQuery(q)) {
                return q;
            }
        }
        throw new CommunicationException("Key is cancelled", error);
    }

    protected AsyncQuery newAsyncQuery(long id, Code code, Object[] args) {
        return new AsyncQuery(id, code, args);
    }

    protected boolean addQuery(AsyncQuery q) {
        futures.put(q.id, q);
        writeQueue.add(q);
        if (key.isValid()) {
            key.selector().wakeup();
            return true;
        } else {
            q.setError(error);
        }
        return false;
    }


    @Override
    public void close() {
        close(null);
    }

    @Override
    public boolean isValid() {
        return key.isValid();
    }

    public void close(Exception e) {
        error = e;
        try {
            if (key != null) {
                key.cancel();
            }
            channel.close();
        } catch (Exception ignored) {
        }
        for (AsyncQuery q : futures.values()) {
            q.setError(error);
        }
    }

    @Override
    public Long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Long schemaId) {
        this.schemaId = schemaId;
    }
}
