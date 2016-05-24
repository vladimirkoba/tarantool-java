package org.tarantool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.tarantool.schema.TarantoolConnectionSchemaAware;

public abstract class TarantoolConnection16Base<Space, Tuple, Operation, Result> extends AbstractTarantoolConnection16<Space, Tuple, Operation, Result> implements TarantoolConnectionSchemaAware {
    protected SocketChannel channel;
    protected ConnectionState in;
    protected ConnectionState out;
    protected String salt;
    protected int msgPackOptions = MsgPackLite.OPTION_UNPACK_NUMBER_AS_LONG | MsgPackLite.OPTION_UNPACK_RAW_AS_STRING;


    public TarantoolConnection16Base(SocketChannel channel) {
        try {
            this.channel = channel;
            this.in = new ConnectionState();
            this.out = new ConnectionState();
            ByteBuffer welcome = in.getWelcomeBuffer();
            readFully(welcome);
            String firstLine = new String(welcome.array(), 0, welcome.position());
            if (!firstLine.startsWith("Tarantool")) {
                channel.close();
                throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'");
            }
            welcome = in.getWelcomeBuffer();
            readFully(welcome);
            this.salt = new String(welcome.array(), 0, welcome.position());
        } catch (IOException e) {
            throw new CommunicationException("Can't connect with tarantool", e);
        }
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

    protected Object readData() {
        readPacket();
        return in.getBody().get(Key.DATA);
    }

    protected void readPacket() {
        readFully(in.getLengthReadBuffer());
        readFully(in.getPacketReadBuffer());
        in.unpack(msgPackOptions);
        long code = (Long) in.getHeader().get(Key.CODE);
        if (code != 0) {
            Object error = in.getBody().get(Key.ERROR);
            throw new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error));
        }
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

    public void ping() {
        noError(exec(Code.PING));
    }


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
            noError(exec(Code.AUTH, Key.USER_NAME, username, Key.TUPLE, auth));

        } catch (NoSuchAlgorithmException e) {
            throw new CommunicationException("Can't use sha-1", e);
        }
    }

    protected void noError(Result r) {

    }


    protected int write(Code code, Object[] args) {
        return write(out.pack(code, args));
    }

    @Override
    public Long getSchemaId() {
        return (Long) in.getHeader().get(Key.SCHEMA_ID);
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }


    public int getMsgPackOptions() {
        return msgPackOptions;
    }

    public void setMsgPackOptions(int msgPackOptions) {
        this.msgPackOptions = msgPackOptions;
    }
}
