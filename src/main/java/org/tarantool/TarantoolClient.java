package org.tarantool;

import java.util.List;
import java.util.concurrent.Future;

public interface TarantoolClient {
    TarantoolConnectionOps<Integer, Object, Object, List> syncOps();

    TarantoolConnectionOps<Integer, Object, Object, Future<List>> asyncOps();

    TarantoolConnectionOps<Integer, Object, Object, Void> fireAndForgetOps();

    void close();

}
