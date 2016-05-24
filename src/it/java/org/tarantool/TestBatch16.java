package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;

import org.tarantool.batch.BatchedQueryResult;
import org.tarantool.batch.TarantoolBatchConnection16;
import org.tarantool.batch.TarantoolBatchConnection16Impl;
import org.tarantool.schema.SchemaResolver;

public class TestBatch16 {
    /*
     Before executing this test you should configure your local tarantool

     box.cfg{listen=3301}
     box.schema.space.create('tester')
     box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

     box.schema.user.create('test', { password = 'test' })
     box.schema.user.grant('test', 'execute,read,write', 'universe')
     box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}


    */
    public static void main(String[] args) throws IOException {
        TarantoolBatchConnection16 con = new TarantoolBatchConnection16Impl(SocketChannel.open(new InetSocketAddress("localhost", 3301)));
        con.auth("test", "test");

        TarantoolConnection16 c = new TarantoolConnection16Impl("localhost", 3301);
        c.auth("test", "test");

        final TestSchema schema = new SchemaResolver().schema(new TestSchema(), c);
        c.close();
        System.out.println(schema);


        test(con, schema);
        test(con, schema);
        test(con, schema);

        con.close();

    }

    private static void test(TarantoolBatchConnection16 con, TestSchema schema) {
        con.begin();
        BatchedQueryResult delete0 = con.delete(schema.tester.id, Arrays.asList(0));

        BatchedQueryResult delete = con.delete(schema.tester.id, Arrays.asList(1));

        BatchedQueryResult insert = con.insert(schema.tester.id, Arrays.asList(1, "hello"));

        BatchedQueryResult insert2 = con.replace(schema.tester.id, Arrays.asList(2, Collections.singletonMap("hello", "word"),new String[]{"a","b","c"}));

        BatchedQueryResult select0 = con.select(schema.tester.id, schema.tester.primary, Arrays.asList(1), 0, 100, 0);

        BatchedQueryResult update0 = con.update(schema.tester.id, Arrays.asList(1), Arrays.asList("=", 1, "Hello"));

        BatchedQueryResult result = con.call("math.ceil", 1.3);

        BatchedQueryResult eval = con.eval("return ...", 1, 2, 3);

        con.end();
        con.get();
        System.out.println(delete0);
        System.out.println(delete);
        System.out.println(insert);
        System.out.println(insert2);
        System.out.println(select0);
        System.out.println(update0);
        System.out.println(result);
        System.out.println(eval);
    }
}
