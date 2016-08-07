package org.tarantool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface TarantoolClient {
    TarantoolConnection16Ops<Integer, Object, Object, List> syncOps();

    TarantoolConnection16Ops<Integer, Object, Object, Future<List>> asyncOps();

    TarantoolConnection16Ops<Integer, Object, Object, Void> fireAndForgetOps();

    void close();

}
