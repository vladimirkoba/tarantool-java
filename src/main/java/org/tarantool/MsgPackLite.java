package org.tarantool;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Updated MsgPackLite class with corrected handling of extension types.
 */
public class MsgPackLite {

    public static final MsgPackLite INSTANCE = new MsgPackLite();

    protected static final int MAX_4BIT = 0xf;
    protected static final int MAX_5BIT = 0x1f;
    protected static final int MAX_7BIT = 0x7f;
    protected static final int MAX_8BIT = 0xff;
    protected static final int MAX_15BIT = 0x7fff;
    protected static final int MAX_16BIT = 0xffff;
    protected static final int MAX_31BIT = 0x7fffffff;
    protected static final long MAX_32BIT = 0xffffffffL;

    protected static final BigInteger BI_MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    protected static final BigInteger BI_MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    protected static final BigInteger BI_MAX_64BIT = BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE);

    // MessagePack type constants (defined as int for correct comparisons)
    protected static final int MP_NULL = 0xc0;
    protected static final int MP_FALSE = 0xc2;
    protected static final int MP_TRUE = 0xc3;
    protected static final int MP_BIN8 = 0xc4;
    protected static final int MP_BIN16 = 0xc5;
    protected static final int MP_BIN32 = 0xc6;

    protected static final int MP_FLOAT = 0xca;
    protected static final int MP_DOUBLE = 0xcb;

    protected static final int MP_FIXNUM = 0x00; // last 7 bits is value
    protected static final int MP_UINT8 = 0xcc;
    protected static final int MP_UINT16 = 0xcd;
    protected static final int MP_UINT32 = 0xce;
    protected static final int MP_UINT64 = 0xcf;

    protected static final int MP_NEGATIVE_FIXNUM = 0xe0; // last 5 bits is value
    protected static final int MP_NEGATIVE_FIXNUM_INT = 0xe0;
    protected static final int MP_INT8 = 0xd0;
    protected static final int MP_INT16 = 0xd1;
    protected static final int MP_INT32 = 0xd2;
    protected static final int MP_INT64 = 0xd3;

    protected static final int MP_FIXARRAY = 0x90; // last 4 bits is size
    protected static final int MP_FIXARRAY_INT = 0x90;
    protected static final int MP_ARRAY16 = 0xdc;
    protected static final int MP_ARRAY32 = 0xdd;

    protected static final int MP_FIXMAP = 0x80; // last 4 bits is size
    protected static final int MP_FIXMAP_INT = 0x80;
    protected static final int MP_MAP16 = 0xde;
    protected static final int MP_MAP32 = 0xdf;

    protected static final int MP_FIXSTR = 0xa0; // last 5 bits is size
    protected static final int MP_FIXSTR_INT = 0xa0;
    protected static final int MP_STR8 = 0xd9;
    protected static final int MP_STR16 = 0xda;
    protected static final int MP_STR32 = 0xdb;

    // Extension Types
    protected static final int MP_EXT8 = 0xc7;
    protected static final int MP_EXT16 = 0xc8;
    protected static final int MP_EXT32 = 0xc9;
    protected static final int MP_FIXEXT1 = 0xd4;
    protected static final int MP_FIXEXT2 = 0xd5;
    protected static final int MP_FIXEXT4 = 0xd6;
    protected static final int MP_FIXEXT8 = 0xd7;
    protected static final int MP_FIXEXT16 = 0xd8;

    // Define the UUID extension type code as per Tarantool's documentation
    protected static final int MP_UUID_TYPE = 2;

    public void pack(Object item, OutputStream os) throws IOException {
        DataOutputStream out = new DataOutputStream(os);

        // Handle Callable items first
        if (item instanceof Callable) {
            try {
                item = ((Callable<?>) item).call();
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        // Handle null
        if (item == null) {
            out.write(MP_NULL);
        }
        // Handle Boolean
        else if (item instanceof Boolean) {
            out.write((Boolean) item ? MP_TRUE : MP_FALSE);
        }
        // Handle UUID as Extension Type
        else if (item instanceof UUID) {
            UUID uuid = (UUID) item;
            byte[] uuidBytes = uuidToBytes(uuid);
            // Serialize as FIXEXT16 (type code 2, 16 bytes)
            out.write(MP_FIXEXT16);
            out.write(MP_UUID_TYPE); // Type code for UUID
            out.write(uuidBytes);
        }
        // Handle Numbers and Code
        else if (item instanceof Number || item instanceof Code) {
            if (item instanceof Float) {
                out.write(MP_FLOAT);
                out.writeFloat((Float) item);
            } else if (item instanceof Double) {
                out.write(MP_DOUBLE);
                out.writeDouble((Double) item);
            } else {
                if (item instanceof BigInteger) {
                    BigInteger value = (BigInteger) item;
                    boolean isPositive = value.signum() >= 0;
                    if ((isPositive && value.compareTo(BI_MAX_64BIT) > 0) ||
                        value.compareTo(BI_MIN_LONG) < 0) {
                        throw new IllegalArgumentException(
                            "Cannot encode BigInteger as MsgPack: out of -2^63..2^64-1 range");
                    }
                    if (isPositive && value.compareTo(BI_MAX_LONG) > 0) {
                        byte[] data = value.toByteArray();
                        // data can contain leading zero bytes
                        for (int i = 0; i < data.length - 8; ++i) {
                            if (data[i] != 0) {
                                throw new IllegalArgumentException(
                                    "Cannot encode BigInteger as MsgPack: out of -2^63..2^64-1 range");
                            }
                        }
                        out.write(MP_UINT64);
                        out.write(data, data.length - 8, 8);
                        return;
                    }
                }
                long value = item instanceof Code ? ((Code) item).getId() : ((Number) item).longValue();
                if (value >= 0) {
                    if (value <= MAX_7BIT) {
                        out.write((int) value | MP_FIXNUM);
                    } else if (value <= MAX_8BIT) {
                        out.write(MP_UINT8);
                        out.write((int) value);
                    } else if (value <= MAX_16BIT) {
                        out.write(MP_UINT16);
                        out.writeShort((int) value);
                    } else if (value <= MAX_32BIT) {
                        out.write(MP_UINT32);
                        out.writeInt((int) value);
                    } else {
                        out.write(MP_UINT64);
                        out.writeLong(value);
                    }
                } else {
                    if (value >= -(MAX_5BIT + 1)) {
                        out.write((int) (value & 0xff));
                    } else if (value >= -(MAX_7BIT + 1)) {
                        out.write(MP_INT8);
                        out.write((int) value);
                    } else if (value >= -(MAX_15BIT + 1)) {
                        out.write(MP_INT16);
                        out.writeShort((int) value);
                    } else if (value >= -(MAX_31BIT + 1)) {
                        out.write(MP_INT32);
                        out.writeInt((int) value);
                    } else {
                        out.write(MP_INT64);
                        out.writeLong(value);
                    }
                }
            }
        }
        // Handle String
        else if (item instanceof String) {
            byte[] data = ((String) item).getBytes("UTF-8");
            if (data.length <= MAX_5BIT) {
                out.write(data.length | MP_FIXSTR);
            } else if (data.length <= MAX_8BIT) {
                out.write(MP_STR8);
                out.writeByte(data.length);
            } else if (data.length <= MAX_16BIT) {
                out.write(MP_STR16);
                out.writeShort(data.length);
            } else {
                out.write(MP_STR32);
                out.writeInt(data.length);
            }
            out.write(data);
        }
        // Handle byte[] and ByteBuffer
        else if (item instanceof byte[] || item instanceof ByteBuffer) {
            byte[] data;
            if (item instanceof byte[]) {
                data = (byte[]) item;
            } else {
                ByteBuffer bb = ((ByteBuffer) item);
                if (bb.hasArray()) {
                    data = bb.array();
                } else {
                    data = new byte[bb.capacity()];
                    bb.position(0);
                    bb.limit(bb.capacity());
                    bb.get(data);
                }
            }
            if (data.length <= MAX_8BIT) {
                out.write(MP_BIN8);
                out.writeByte(data.length);
            } else if (data.length <= MAX_16BIT) {
                out.write(MP_BIN16);
                out.writeShort(data.length);
            } else {
                out.write(MP_BIN32);
                out.writeInt(data.length);
            }
            out.write(data);
        }
        // Handle List and Arrays
        else if (item instanceof List || item.getClass().isArray()) {
            int length = item instanceof List ? ((List<?>) item).size() : Array.getLength(item);
            if (length <= MAX_4BIT) {
                out.write(length | MP_FIXARRAY);
            } else if (length <= MAX_16BIT) {
                out.write(MP_ARRAY16);
                out.writeShort(length);
            } else {
                out.write(MP_ARRAY32);
                out.writeInt(length);
            }
            if (item instanceof List) {
                List<?> list = ((List<?>) item);
                for (Object element : list) {
                    pack(element, out);
                }
            } else {
                for (int i = 0; i < length; i++) {
                    pack(Array.get(item, i), out);
                }
            }
        }
        // Handle Map
        else if (item instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) item;
            if (map.size() <= MAX_4BIT) {
                out.write(map.size() | MP_FIXMAP);
            } else if (map.size() <= MAX_16BIT) {
                out.write(MP_MAP16);
                out.writeShort(map.size());
            } else {
                out.write(MP_MAP32);
                out.writeInt(map.size());
            }
            for (Map.Entry<?, ?> kvp : map.entrySet()) {
                pack(kvp.getKey(), out);
                pack(kvp.getValue(), out);
            }
        }
        // Unsupported type
        else {
            throw new IllegalArgumentException("Cannot msgpack object of type " + item.getClass().getCanonicalName());
        }
    }

    public Object unpack(InputStream is) throws IOException {
        DataInputStream in = new DataInputStream(is);
        int value = in.read();
        if (value < 0) {
            throw new IllegalArgumentException("No more input available when expecting a value");
        }
        switch (value) {
            case MP_NULL:
                return null;
            case MP_FALSE:
                return false;
            case MP_TRUE:
                return true;
            case MP_FLOAT:
                return in.readFloat();
            case MP_DOUBLE:
                return in.readDouble();
            case MP_UINT8:
                return in.readUnsignedByte();
            case MP_UINT16:
                return in.readUnsignedShort();
            case MP_UINT32:
                return (long) in.readInt() & MAX_32BIT;
            case MP_UINT64: {
                long v = in.readLong();
                if (v >= 0) {
                    return v;
                } else {
                    // Handle unsigned long by converting to BigInteger
                    byte[] bytes = new byte[] {
                        (byte) ((v >> 56) & 0xff),
                        (byte) ((v >> 48) & 0xff),
                        (byte) ((v >> 40) & 0xff),
                        (byte) ((v >> 32) & 0xff),
                        (byte) ((v >> 24) & 0xff),
                        (byte) ((v >> 16) & 0xff),
                        (byte) ((v >> 8) & 0xff),
                        (byte) (v & 0xff),
                    };
                    return new BigInteger(1, bytes);
                }
            }
            case MP_INT8:
                return (int) in.readByte();
            case MP_INT16:
                return (int) in.readShort();
            case MP_INT32:
                return in.readInt();
            case MP_INT64:
                return in.readLong();
            case MP_FIXARRAY:
                return unpackList(value - MP_FIXARRAY_INT, in);
            case MP_ARRAY16:
                return unpackList(in.readUnsignedShort(), in);
            case MP_ARRAY32:
                return unpackList(in.readInt(), in);
            case MP_FIXMAP:
                return unpackMap(value - MP_FIXMAP_INT, in);
            case MP_MAP16:
                return unpackMap(in.readUnsignedShort(), in);
            case MP_MAP32:
                return unpackMap(in.readInt(), in);
            case MP_FIXSTR:
                return unpackStr(value - MP_FIXSTR_INT, in);
            case MP_STR8:
                return unpackStr(in.readUnsignedByte(), in);
            case MP_STR16:
                return unpackStr(in.readUnsignedShort(), in);
            case MP_STR32:
                return unpackStr(in.readInt(), in);
            case MP_BIN8:
                return unpackBin(in.readUnsignedByte(), in);
            case MP_BIN16:
                return unpackBin(in.readUnsignedShort(), in);
            case MP_BIN32:
                return unpackBin(in.readInt(), in);
            case MP_FIXEXT1:
            case MP_FIXEXT2:
            case MP_FIXEXT4:
            case MP_FIXEXT8:
            case MP_FIXEXT16:
            case MP_EXT8:
            case MP_EXT16:
            case MP_EXT32:
                return unpackExt(value, in);
            default:
                break;
        }

        if (value >= MP_NEGATIVE_FIXNUM_INT) {
            return (byte) (value - 256); // Convert to signed byte
        } else if (value >= MP_FIXARRAY_INT && value <= MP_FIXARRAY_INT + MAX_4BIT) {
            return unpackList(value - MP_FIXARRAY_INT, in);
        } else if (value >= MP_FIXMAP_INT && value <= MP_FIXMAP_INT + MAX_4BIT) {
            return unpackMap(value - MP_FIXMAP_INT, in);
        } else if (value >= MP_FIXSTR_INT && value <= MP_FIXSTR_INT + MAX_5BIT) {
            return unpackStr(value - MP_FIXSTR_INT, in);
        } else if (value <= MAX_7BIT) {
            // MP_FIXNUM - the value is value as an int
            return value;
        } else {
            throw new IllegalArgumentException("Input contains invalid type value " + String.format("0x%02x", value));
        }
    }

    protected List<Object> unpackList(int size, DataInputStream in) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("Array to unpack too large for Java (more than 2^31 elements)!");
        }
        List<Object> ret = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            ret.add(unpack(in));
        }
        return ret;
    }

    protected Map<Object, Object> unpackMap(int size, DataInputStream in) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("Map to unpack too large for Java (more than 2^31 elements)!");
        }
        Map<Object, Object> ret = new HashMap<>(size);
        for (int i = 0; i < size; ++i) {
            Object key = unpack(in);
            Object value = unpack(in);
            ret.put(key, value);
        }
        return ret;
    }

    protected String unpackStr(int size, DataInputStream in) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("String to unpack too large for Java (more than 2^31 elements)!");
        }

        byte[] data = new byte[size];
        in.readFully(data);
        return new String(data, "UTF-8");
    }

    protected byte[] unpackBin(int size, DataInputStream in) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("byte[] to unpack too large for Java (more than 2^31 elements)!");
        }

        byte[] data = new byte[size];
        in.readFully(data);
        return data;
    }

    /**
     * Handle Extension Types during unpacking.
     *
     * @param format The format byte indicating the type of extension.
     * @param in     The input stream.
     * @return The deserialized object, e.g., UUID.
     * @throws IOException If an I/O error occurs.
     */
    protected Object unpackExt(int format, DataInputStream in) throws IOException {
        int length;
        int type;

        switch (format) {
            case MP_FIXEXT1:
                length = 1;
                type = in.readUnsignedByte();
                break;
            case MP_FIXEXT2:
                length = 2;
                type = in.readUnsignedByte();
                break;
            case MP_FIXEXT4:
                length = 4;
                type = in.readUnsignedByte();
                break;
            case MP_FIXEXT8:
                length = 8;
                type = in.readUnsignedByte();
                break;
            case MP_FIXEXT16:
                length = 16;
                type = in.readUnsignedByte();
                break;
            case MP_EXT8:
                length = in.readUnsignedByte();
                type = in.readUnsignedByte();
                break;
            case MP_EXT16:
                length = in.readUnsignedShort();
                type = in.readUnsignedByte();
                break;
            case MP_EXT32:
                length = in.readInt();
                type = in.readUnsignedByte();
                break;
            default:
                throw new IllegalArgumentException("Unknown extension format: " + String.format("0x%02x", format));
        }

        // Handle UUID Extension Type as per Tarantool's documentation
        if (type == MP_UUID_TYPE && length == 16) {
            byte[] uuidBytes = new byte[16];
            in.readFully(uuidBytes);
            return bytesToUUID(uuidBytes);
        } else {
            // For other extension types, return as byte array or handle accordingly
            byte[] extData = new byte[length];
            in.readFully(extData);
            // You can return a custom object or a byte array; here we return a byte array
            return extData;
        }
    }

    /**
     * Convert a UUID to a 16-byte array.
     *
     * @param uuid The UUID to convert.
     * @return A 16-byte array representing the UUID.
     */
    protected byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * Convert a 16-byte array to a UUID.
     *
     * @param bytes The byte array to convert.
     * @return The resulting UUID.
     */
    protected UUID bytesToUUID(byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("UUID byte array must be 16 bytes long");
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }
}
