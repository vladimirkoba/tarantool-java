package org.tarantool.named;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.tarantool.Code;
import org.tarantool.batch.BatchedQuery;
import org.tarantool.batch.BatchedQueryResult;
import org.tarantool.batch.TarantoolBatchConnection16Impl;

public class TarantoolNamedBatchConnection16Impl extends TarantoolNamedBase16<BatchedQueryResult> implements TarantoolNamedBatchConnection16 {
    protected TarantoolBatchConnection16Impl delegate;
    protected long schemaId;

    public TarantoolNamedBatchConnection16Impl(SocketChannel channel) {
        delegate = new TarantoolBatchConnection16Impl(channel) {
            @Override
            protected void write(BatchedQuery q) {
                write(in.pack(q.code, q.id, isCodeResolvable(q.code) ? schemaId : 0, resolveArgs(q.code, q.args)));
            }
        };
    }

    @Override
    public BatchedQueryResult exec(Code code, Object... args) {
        return delegate.exec(code, args);
    }

    @Override
    public void begin() {
        delegate.begin();
    }

    @Override
    public void end() {
        delegate.end();
    }

    @Override
    public void get() {
        List<BatchedQuery> qs = new ArrayList<BatchedQuery>(delegate.getBatch().values());
        delegate.get();
        List<BatchedQuery> failedBySchema = null;
        for (int i = 0; i < qs.size(); i++) {
            BatchedQuery q = qs.get(i);
            if (q.result.getError() == null) {
                q.result.setResult(resolveTuples(q.code, q.args, q.result.getResult()));
            } else {
                if (q.result.getError().getCode() == ER_SCHEMA_CHANGED) {
                    if (failedBySchema == null) {
                        failedBySchema = new ArrayList<BatchedQuery>(qs.size() - i);
                    }
                    q.result.setError(null);
                    failedBySchema.add(q);
                }
            }
        }
        if (failedBySchema != null) {
            updateSchema();
            delegate.begin();
            for (BatchedQuery q : failedBySchema) {
                delegate.addQuery(q);
            }
            delegate.end();
            get();
        }
    }

    @Override
    public void auth(String username, String password) {
        delegate.auth(username, password);
        updateSchema();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public long getSchemaId() {
        return schemaId;
    }

    protected List<List> select(int space) {
        return delegate.select(space, 0, Collections.emptyList(), 0, 1000, 0).getResult();
    }

    protected void updateSchema() {
        schemaId = 0L;
        buildSchema(select(VSPACE), select(VINDEX));
        schemaId = delegate.getSchemaId();
    }

    public void setSchemaId(long schemaId) {
        this.schemaId = schemaId;
    }
}
