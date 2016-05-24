package org.tarantool.named;

import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;

import org.tarantool.Code;
import org.tarantool.TarantoolConnection16Impl;
import org.tarantool.TarantoolException;

public class TarantoolNamedConnection16Impl extends TarantoolNamedBase16<List> implements TarantoolNamedConnection16 {
    protected TarantoolConnection16Impl delegate;
    protected long schemaId;

    public TarantoolNamedConnection16Impl(SocketChannel channel) {
        delegate = new TarantoolConnection16Impl(channel) {
            @Override
            protected int write(Code code, Object[] args) {
                return write(in.pack(code, null, isCodeResolvable(code) ? schemaId : null, args));
            }
        };
    }

    public List exec(Code code, Object... args) {
        Object[] mutableArgs = resolveArgs(code, args);
        try {
            List<List> tuples = delegate.exec(code, mutableArgs);
            List resolved = resolveTuples(code, args, tuples);
            return resolved == null ? tuples : resolved;
        } catch (TarantoolException e) {
            if (e.getCode() == ER_SCHEMA_CHANGED) {
                updateSchema();
                return exec(code, args);
            }
            throw e;
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
        return delegate.select(space, 0, Collections.emptyList(), 0, 1000, 0);
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
