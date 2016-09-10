package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class TestTarantoolClient {
    /*
      Before executing this test you should configure your local tarantool

      box.cfg{listen=3301}
      space = box.schema.space.create('tester')
      box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

      box.schema.user.create('test', { password = 'test' })
      box.schema.user.grant('test', 'execute,received,write', 'universe')
      box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}
     */
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, SQLException {
        SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
        final int calls = 120000;
        final AtomicLong c = new AtomicLong(0);
        final Semaphore s = new Semaphore(0);
        TarantoolClientOptions options = new TarantoolClientOptions();
        final TarantoolClientImpl client = new TarantoolClientImpl(channel, options) {
            @Override
            protected void complete(long code, FutureImpl<List> q) {
                super.complete(code, q);
                c.incrementAndGet();
                s.release();
            }
        };

        TarantoolConnectionOps<Integer, Object, Object, List> con = client.syncOps();
        con.auth("test", "test");

        long st = System.currentTimeMillis();
        final int threads = 16;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for (long i = 0; i < Math.ceil((double) calls / threads); i++) {
                        client.fireAndForgetOps().replace(512, Arrays.asList(i % 10000, "hello"));

                    }
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("pushed " + (System.currentTimeMillis() - st) + "ms \n" + client.stats.toString());
        s.acquire(calls);
        client.close();
        System.out.println("completed "+(System.currentTimeMillis() - st) + "ms \n" + client.stats.toString());

    }
}
