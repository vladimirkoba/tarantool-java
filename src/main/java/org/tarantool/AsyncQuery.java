package org.tarantool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.tarantool.Code;

public class AsyncQuery<V> implements Future<V> {
    protected Long id;
    protected Code code;
    protected Object[] args;
    protected V value;
    protected Exception error;
    protected CountDownLatch latch = new CountDownLatch(1);

    public AsyncQuery(Long id, Code code, Object[] args) {
        this.id = id;
        this.code = code;
        this.args = args;
    }

    protected AsyncQuery() {
    }

    public void setError(Exception e) {
        error = e;
        latch.countDown();
    }

    public void setValue(V v) {
        value = v;
        latch.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0L;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        latch.await();
        if (error != null) {
            throw new ExecutionException(error);
        }
        return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        latch.await(timeout, unit);
        if (error != null) {
            throw new ExecutionException(error);
        }
        return value;
    }

    public Code getCode() {
        return code;
    }

    public Object[] getArgs() {
        return args;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
}
