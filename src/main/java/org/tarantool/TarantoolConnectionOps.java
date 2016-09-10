package org.tarantool;


public interface TarantoolConnectionOps<T,O,P,R> {
    R select(T space, T index, O key, int offset, int limit, int iterator);


    R insert(T space, O tuple);


    R replace(T space, O tuple);


    R update(T space, O key, P... tuple);

    R upsert(T space, O key, O defTuple, P... ops);

    R delete(T space, O key);

    R call(String function, Object... args);

    R eval(String expression, Object... args);

    void auth(String username, String password);

    void ping();

    void close();
}
