package org.tarantool;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;

public class ConnectionState {

    protected EnumMap<Key, Object> header = new EnumMap<Key, Object>(Key.class);
    protected EnumMap<Key, Object> body = new EnumMap<Key, Object>(Key.class);

    protected ByteBufferStreams buffer = new ByteBufferStreams(ByteBuffer.allocate(32 * 1024), 1.1d);


    public ByteBuffer getWelcomeBuffer() {
        ByteBuffer buf = buffer.getBuf();
        buf.clear();
        buf.limit(64);
        return buf;
    }

    public ByteBuffer getLengthReadBuffer() {
        ByteBuffer buf = buffer.getBuf();
        buf.clear();
        buf.limit(5);
        return buf;
    }

    public ByteBuffer getPacketReadBuffer() {
        ByteBuffer buf = buffer.getBuf();
        buf.limit(buf.position());
        buf.rewind();
        try {
            long size = (Long) MsgPackLite.unpack(buffer.asInputStream(), 0);
            buf.clear();
            buffer.checkCapacity((int) size);
            buf = buffer.getBuf();
            buf.position(0);
            buf.limit((int)size);
            return buf;
        } catch (IOException e) {
            //this shouldn't happens
            throw new IllegalStateException(e);
        }
    }

    public void unpack() {
        ByteBuffer buf = buffer.getBuf();
        buf.limit(buf.position());
        buf.rewind();
        body.clear();
        header.clear();
        try {
            toKeyMap(MsgPackLite.unpack(buffer.asInputStream(), MsgPackLite.OPTION_UNPACK_RAW_AS_STRING), header);
            if (buf.remaining() > 0) {
                toKeyMap(MsgPackLite.unpack(buffer.asInputStream(), MsgPackLite.OPTION_UNPACK_RAW_AS_STRING), body);
            }
        } catch (IOException e) {
            //this shouldn't happens
            throw new IllegalStateException(e);
        }
    }

    private void toKeyMap(Object unpack, EnumMap<Key, Object> result) {
        Map<Integer, Object> map = (Map<Integer, Object>) unpack;
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            result.put(Key.getById(entry.getKey()), entry.getValue());
        }
    }


    public ByteBuffer pack(Code code, Object[] args) {
        beginBody();
        if (args != null) {
            for (int i = 0, e = args.length; i < e; i += 2) {
                Object value = args[i + 1];
                put((Key) args[i], value);
            }
        }
        endBody();
        return pack(code, null, body);
    }

    public ByteBuffer pack(Code code, Object sync, EnumMap<Key, Object> body) {
        header.clear();
        header.put(Key.CODE, code);
        if (sync != null) {
            header.put(Key.SYNC, sync);
        }

        ByteBuffer buf = buffer.getBuf();
        buf.clear();
        buf.put((byte) 0xce);
        buf.putInt(0);
        try {
            MsgPackLite.pack(header, buffer.asOutputStream());
            MsgPackLite.pack(body, buffer.asOutputStream());
        } catch (IOException e) {
            //this shouldn't happens
            throw new IllegalStateException(e);
        }
        //buffer can be changed during write process
        buf = buffer.getBuf();
        int size = buf.position();
        buf.putInt(1, size - 5);
        buf.limit(size);
        buf.rewind();
        return buf;
    }

    public ConnectionState beginBody() {
        body.clear();
        return this;
    }

    public ConnectionState put(Key key, Object value) {
        if (value != null) {
            body.put(key, value);
        }
        return this;
    }

    public EnumMap<Key, Object> endBody() {
        return body;
    }

    public EnumMap<Key, Object> getHeader() {
        return header;
    }

    public EnumMap<Key, Object> getBody() {
        return body;
    }
}
