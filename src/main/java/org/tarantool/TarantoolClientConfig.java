package org.tarantool;


public class TarantoolClientConfig {

    /**
     * username and password for authorization
     */
    public String username;

    public String password;

    /**
     * default ByteArrayOutputStream size  when make query serialization
     */
    public int defaultRequestSize = 1024;

    /**
     * MessagePack options when reading from upstream
     */
    public int msgPackOptions = MsgPackLite.OPTION_UNPACK_NUMBER_AS_LONG | MsgPackLite.OPTION_UNPACK_RAW_AS_STRING;

    /**
     * initial size for map which holds futures of sent request
     */
    public int predictedFutures = (int) ((1024 * 1024) / 0.75) + 1;


    public int writerThreadPriority = Thread.NORM_PRIORITY;

    public int readerThreadPriority = Thread.NORM_PRIORITY;


    /**
     * shared buffer is place where client collect requests when socket is busy on write
     */
    public int sharedBufferSize = 8 * 1024 * 1024;
    /**
     * not put request into the shared buffer if request size is ge directWriteFactor * sharedBufferSize
     */
    public double directWriteFactor = 0.5d;

    /**
     *
     */
    public boolean useOldCall = true;
}
