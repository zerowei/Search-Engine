package edu.uci.ics.cs221.index.inverted;

import java.nio.ByteBuffer;

import static edu.uci.ics.cs221.index.inverted.PageFileChannel.PAGE_SIZE;

public class AutoFlushBuffer {
    ByteBuffer buffer;
    PageFileChannel file;
    int pageId, offset;

    AutoFlushBuffer(PageFileChannel file) {
        this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        this.file = file;
    }

    AutoFlushBuffer put(byte oneByte) {
        buffer.put(oneByte);

        if (buffer.hasRemaining() == false) {
            flush();
        }

        pageId = pageId + (offset+1) / PAGE_SIZE;
        offset = (offset+1) % PAGE_SIZE;

        return this;
    }

    AutoFlushBuffer put(byte[] bytes) {
        for (byte b : bytes) {
            put(b);
        }

        return this;
    }

    AutoFlushBuffer put(ByteBuffer bytes) {
        bytes.rewind();

        while (bytes.hasRemaining()) {
            byte[] tempByte = new byte[1];
            bytes.get(tempByte);
            put(tempByte);
        }

        return this;
    }

    AutoFlushBuffer putInt(int i) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(i);
        return put(buffer);
    }

    AutoFlushBuffer flush() {
        byte byteZero = 0;
        while (buffer.hasRemaining()) {
            buffer.put(byteZero);
        }

        file.appendAllBytes(buffer);
        buffer.clear();
        return this;
    }

    int getPageId() {
        return pageId;
    }

    int getOffset() {
        return offset;
    }
}

