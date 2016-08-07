package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.tarantool.jdbc.Connection;


public class TestTarantoolClient {
    /*
      Before executing this test you should configure your local tarantool

      box.cfg{listen=3301}
      space = box.schema.space.create('tester')
      box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

      box.schema.user.create('test', { password = 'test' })
      box.schema.user.grant('test', 'execute,read,write', 'universe')
      box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}
     */
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, SQLException {
        SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
        final int calls = 20000;
        final AtomicLong c = new AtomicLong(0);
        final Semaphore s = new Semaphore(0);
        final TarantoolClient client = new TarantoolClientImpl(channel, 1, 32768) {
            @Override
            protected void complete(long code, AsyncQuery<List> q) {
                super.complete(code, q);
                if(q == null) {
                    System.out.println("AAA");
                }
                c.incrementAndGet();
                s.release();
            }
        };
        TarantoolConnection16Ops<Integer, Object, Object, List> con = client.syncOps();
        con.auth("test", "test");
        Connection connection = new Connection(client, new Properties());
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT a+b as hello,c,d FROM t1 WHERE b=3000 OR c=6");
        System.out.println(rs.next());
        ResultSet rs2 = statement.executeQuery("SELECT a+b as hello,c,d FROM t1");
        System.out.println(rs2);
        rs2.next();
        System.out.println(rs2.getString("hello"));
        System.out.println(rs2.getBoolean("c"));
        System.out.println(rs2.getInt("d"));


        final int threads = 1;
        long st = System.currentTimeMillis();
        for (long i = 0; i < Math.ceil((double) calls / threads); i++) {
            client.asyncOps().replace(513,Arrays.asList(i%10000,"hello"));
        }
        s.acquire(calls);
        System.out.println(System.currentTimeMillis() - st + " " + c);
        client.close();

    }
}
