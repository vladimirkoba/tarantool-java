package org.tarantool;


public class TarantoolClientStats {
    final long start = System.currentTimeMillis();
    public volatile long buffered;
    public volatile long received;
    public volatile long bufferedWrites;
    public volatile long directWrite;

    @Override
    public String toString() {
        return "TarantoolClientStats" +
                "\nrunning = " + (System.currentTimeMillis() - start) + "ms" +
                "\nbuffered = " + buffered +
                "\nreceived = " + received +
                "\ndirectWrite = " + directWrite +
                "\nbufferedWrites = " + bufferedWrites + "\n";
    }
}
