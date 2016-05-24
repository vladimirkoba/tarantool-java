package org.tarantool.named;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.tarantool.Code;
import org.tarantool.CommunicationException;
import org.tarantool.Key;
import org.tarantool.TarantoolException;
import org.tarantool.async.AsyncQuery;
import org.tarantool.async.TarantoolAsyncConnection16Impl;
import org.tarantool.async.TarantoolSelectorWorker;

public class TarantoolAsyncNamedConnection16Impl extends TarantoolNamedBase16<Future<List>> implements TarantoolAsyncNamedConnection16{
    protected TarantoolAsyncConnection16Impl delegate;
    protected Future<List> spaces;
    protected Future<List> indexes;
    protected Semaphore schemaUpdate = new Semaphore(1);
    protected BlockingQueue<AsyncQuery> reResovle = new LinkedBlockingDeque<AsyncQuery>();

    private static class AsyncQueryEx<V> extends AsyncQuery<V> {
        Map<String, String> fields;
        Long schemaId;

        public AsyncQueryEx(Long id, Code code, Object[] args) {
            super(id, code, args);
        }

        @Override
        public String toString() {
            return "AsyncQueryEx{code=" + code.name()+", " +
                    " schemaId=" + schemaId +
                    '}';
        }
    }

    public TarantoolAsyncNamedConnection16Impl(TarantoolSelectorWorker worker, SocketChannel channel, String username, String password, long timeout, TimeUnit unit) {
        delegate = new TarantoolAsyncConnection16Impl(worker, channel, username, password, timeout, unit) {

            @Override
            public void auth(String username, String password) {
                super.auth(username, password);
                try {
                    buildSchema(select(VSPACE, 0, Collections.emptyList(), 0, 1000, 0).get(),select(VINDEX, 0, Collections.emptyList(), 0, 1000, 0).get());
                } catch (Exception e) {
                    throw new IllegalStateException("Can't resolve schema", e);
                }
            }

            @Override
            protected ByteBuffer write(AsyncQuery query) {
                if (isSchemaResolveQuery(query) || !isCodeResolvable(query.getCode())) {
                    return writeState.pack(query.getCode(), query.getId(), query.getArgs());
                }
                Object[] args;
                Long schemaId;
                AsyncQueryEx qEx = (AsyncQueryEx) query;
                do {
                    schemaId = getSchemaId();
                    qEx.fields = fields;
                    args = TarantoolAsyncNamedConnection16Impl.this.resolveArgs(query.getCode(), query.getArgs());
                } while (schemaId != null && !schemaId.equals(getSchemaId()));
                qEx.schemaId = schemaId;
                return writeState.pack(query.getCode(), query.getId(), schemaId, args);


            }

            @Override
            protected void setFutureResult(long code, AsyncQuery q) {
                if (q != null) {
                    if (code != 0) {
                        if (code == ER_SCHEMA_CHANGED) {
                            updateSchemaAndExecute(q);
                        } else {
                            Object error = in.getBody().get(Key.ERROR);
                            q.setError(new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error)));
                        }
                    } else {
                        if (isSchemaResolveQuery(q)) {
                            q.setValue(in.getBody().get(Key.DATA));
                            performSchemaUpdate(q);
                        } else {
                            q.setValue(resolveTuplesWithFields(q.getCode(), q.getArgs(), (List) in.getBody().get(Key.DATA), ((AsyncQueryEx)q).fields));
                        }
                    }
                }
            }

            private void performSchemaUpdate(AsyncQuery q) {
                if (spaces.isDone() && indexes.isDone()) {
                    try {
                        buildSchema(spaces.get(), indexes.get());
                        schemaId = (Long) in.getHeader().get(Key.SCHEMA_ID);
                    } catch (Exception e) {
                        delegate.close(e);
                    }
                    schemaUpdate.release();
                    AsyncQuery r;
                    while ((r = reResovle.poll()) != null) {
                        r.setId(syncId.incrementAndGet());
                        if (!addQuery(r)) {
                            r.setError(new CommunicationException("Query execution failed", error));
                        }
                    }
                }
            }

            @Override
            protected AsyncQuery newAsyncQuery(long id, Code code, Object[] args) {
                return new AsyncQueryEx(id, code, args);
            }
        };
    }

    private boolean isSchemaResolveQuery(AsyncQuery q) {
        return q == spaces || q == indexes;
    }

    private void updateSchemaAndExecute(AsyncQuery q) {
        reResovle.add(q);
        if (schemaUpdate.tryAcquire()) {
            spaces = delegate.select(VSPACE, 0, Collections.emptyList(), 0, 1000, 0);
            indexes = delegate.select(VINDEX, 0, Collections.emptyList(), 0, 1000, 0);
        }

    }

    @Override
    public long getSchemaId() {
        return delegate.getSchemaId();
    }

    @Override
    public Future<List> exec(Code code, Object... args) {
        return delegate.exec(code, args);
    }


    @Override
    public void auth(String username, String password) {
        throw new IllegalStateException("Should not be called for async connections");
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isValid() {
        return delegate.isValid();
    }

    public void setSchemaId(long schemaId) {
        delegate.setSchemaId(schemaId);
    }

}
