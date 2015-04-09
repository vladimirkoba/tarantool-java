package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


public class TarantoolConnection16Impl implements TarantoolConnection16 {
    protected final SocketChannel channel;
    protected final ConnectionState state;
    protected final String salt;

    public TarantoolConnection16Impl(String host, int port) {
        try {
            this.channel = SocketChannel.open(new InetSocketAddress(host, port));
            this.state = new ConnectionState();
            ByteBuffer welcome = state.getWelcomeBuffer();
            readFully(welcome);
            String firstLine = new String(welcome.array(), 0, welcome.position());
            if (!firstLine.startsWith("Tarantool")) {
                channel.close();
                throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'");
            }
            welcome = state.getWelcomeBuffer();
            readFully(welcome);
            this.salt = new String(welcome.array(), 0, welcome.position());
        } catch (IOException e) {
            throw new CommunicationException("Can't connect with tarantool", e);
        }
    }


    protected SocketChannel getChannel() {
        return channel;
    }

    protected ConnectionState getState() {
        return state;
    }

    protected int readFully(ByteBuffer buffer) {
        try {
            int code;
            while ((code = channel.read(buffer)) > -1 && buffer.remaining() > 0) {

            }
            if (code < 0) {
                throw new CommunicationException("Can't read bytes");
            }
            return code;
        } catch (IOException e) {
            throw new CommunicationException("Can't read bytes", e);
        }
    }

    protected List exec(Code code, Object... args) {
        write(state.pack(code, args));
        return (List) read();
    }


    protected Object read() {
        readFully(state.getLengthReadBuffer());
        readFully(state.getPacketReadBuffer());
        state.unpack();
        long code = (Long) state.getHeader().get(Key.CODE);
        if (code != 0) {
            Object error = state.getBody().get(Key.ERROR);
            throw new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error));
        }
        return state.getBody().get(Key.DATA);
    }

    protected int write(ByteBuffer buffer) {
        try {
            int code;
            while ((code = channel.write(buffer)) > -1 && buffer.remaining() > 0) {

            }
            if (code < 0) {
                throw new CommunicationException("Can't read bytes");
            }
            return code;
        } catch (IOException e) {
            throw new CommunicationException("Can't write bytes", e);
        }

    }


    @Override
    public List select(int space, int index, Object key, int offset, int limit, int iterator) {
        return exec(Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key, Key.ITERATOR, iterator, Key.LIMIT, limit, Key.OFFSET, offset);
    }

    @Override
    public List insert(int space, Object tuple) {
        return exec(Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public List replace(int space, Object tuple) {
        return exec(Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public List update(int space, Object key, Object... args) {
        return exec(Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, args);
    }

    @Override
    public List delete(int space, Object key) {
        return exec(Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    @Override
    public List call(String function, Object... args) {
        return exec(Code.CALL, Key.FUNCTION, function, Key.TUPLE, args);
    }

    @Override
    public List eval(String expression, Object... args) {
        return exec(Code.EVAL, Key.EXPRESSION, expression, Key.TUPLE, args);
    }

    @Override
    public void auth(String username, final String password) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            List auth = new ArrayList(2);
            auth.add("chap-sha1");

            byte[] p = sha1.digest(password.getBytes());

            sha1.reset();
            byte[] p2 = sha1.digest(p);

            sha1.reset();
            sha1.update(Base64.decode(salt), 0, 20);
            sha1.update(p2);
            byte[] scramble = sha1.digest();
            for (int i = 0, e = 20; i < e; i++) {
                p[i] ^= scramble[i];
            }
            auth.add(p);
            exec(Code.AUTH, Key.USER_NAME, username, Key.TUPLE, auth);

        } catch (NoSuchAlgorithmException e) {
            throw new CommunicationException("Can't use sha-1", e);
        }
    }


    @Override
    public boolean ping() {
        exec(Code.PING);
        return true;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }
}
