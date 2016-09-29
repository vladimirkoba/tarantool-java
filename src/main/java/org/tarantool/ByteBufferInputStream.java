package org.tarantool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class ByteBufferInputStream extends InputStream {
    protected SocketChannel channel;
    protected ByteBuffer buffer;
    protected Selector selector;
    protected long bytesRead;

    public ByteBufferInputStream(SocketChannel channel) throws IOException {
        selector = SelectorProvider.provider().openSelector();
        this.channel = channel;
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
        buffer = ByteBuffer.allocateDirect(channel.socket().getReceiveBufferSize());
        buffer.flip();
    }

    @Override
    public int read() throws IOException {
        if (buffer.remaining() < 1) {
            read(buffer);
        }
        bytesRead++;
        return 0XFF & buffer.get();
    }


    private int read(ByteBuffer buffer) throws IOException {
        buffer.clear();
        int n;
        do {
            n = channel.read(buffer);
            if (n == 0) {
                selector.select();
            } else {
                buffer.flip();
            }

        } while (n == 0);
        if (n < 0) {
            throw new CommunicationException("Channel read failed " + n);
        }
        return n;
    }

    @Override
    public int read(byte[] b, final int off, final int len) throws IOException {
        bytesRead += len;
        if (buffer.remaining() >= len) {
            buffer.get(b, off, len);
            return len;
        } else {
            int i = off;
            int l = len;
            do {
                if (buffer.remaining() == 0) {
                    read(buffer);
                }
                int rem = buffer.remaining();
                buffer.get(b, i, Math.min(rem, l));
                buffer.compact();
                buffer.flip();
                l -= rem;
                i += rem;
            } while (l > 0);
        }
        return len;
    }

    @Override
    public void close() throws IOException {
        selector.close();
        channel.close();
    }
}
