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
    public static class TarantoolClientTestImpl extends TarantoolClientImpl {
        final Semaphore s = new Semaphore(0);

        public TarantoolClientTestImpl(SocketChannelProvider socketProvider, TarantoolClientConfig options) {
            super(socketProvider, options);
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    try {
//                        System.out.println("closed");
//                        channel.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }

        @Override
        protected void reconnect(int retry, Throwable lastError) {
            if (s != null) {
                s.release(wait.get());
            }
            super.reconnect(retry, lastError);
        }

        @Override
        protected void complete(long code, FutureImpl<List> q) {
            super.complete(code, q);
            if (code != 0) {
                System.out.println(code);
            }
            s.release();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, SQLException {
        final int calls = 1000000;

        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = "test";
        config.password = "test";
        //config.sharedBufferSize = 0;
        SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
            @Override
            public SocketChannel get(int retryNumber, Throwable lastError) {
                if (lastError != null) {
                   // lastError.printStackTrace(System.out);
                }
                try {
                    return SocketChannel.open(new InetSocketAddress("vcdevm20.mail.msk", 3301));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
        final TarantoolClientTestImpl client = new TarantoolClientTestImpl(socketChannelProvider, config);
        for (int i = 0; i < 100; i++) {
            long st = System.nanoTime();
            client.syncOps().replace(512, Arrays.asList(i % 10000, "hello"));
            System.out.println(System.nanoTime() - st);

        }
        long st = System.currentTimeMillis();
        final int threads = 16;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for (long i = 0; i < Math.ceil((double) calls / threads); i++) {
                        try {
                            client.asyncOps().replace(512, Arrays.asList(i % 10000, "hello"));
                        } catch (Exception e) {
                            try {
                                client.waitAlive();
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            i--;
                        }

                    }
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("pushed " + (System.currentTimeMillis() - st) + "ms \n" + client.stats.toString());
        client.s.acquire(calls);
        client.close();
        System.out.println("completed " +(System.currentTimeMillis() - st) + "ms \n" + client.stats.toString());

    }
}
