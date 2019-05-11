package edu.uci.ics.cs221.index.inverted;

import java.nio.ByteBuffer;

import static edu.uci.ics.cs221.index.inverted.PageFileChannel.PAGE_SIZE;

public class AutoLoadBuffer {
    ByteBuffer buffer;
    int pageId;
    PageFileChannel file;

    AutoLoadBuffer(PageFileChannel file) {
        pageId = 0;
        this.file = file;

        buffer = ByteBuffer.allocate(PAGE_SIZE);
        if (pageId < file.getNumPages()) {
            buffer = file.readPage(0);
        }
    }

    byte getByte() {
        byte[] result = new byte[1];
        buffer.get(result);

        if (buffer.hasRemaining() == false) {
            buffer.clear();
            pageId++;

            if (pageId < file.getNumPages()) {
                //System.out.println("loading page " + pageId + " \\ " + file.getNumPages());
                buffer = file.readPage(pageId);
            }
        }
        //System.out.println("got byte: " + result[0]);
        return result[0];
    }

    int getInt() {
        ByteBuffer tempBuffer = ByteBuffer.allocate(4);
        for (int i = 0; i < 4; i++) {
            tempBuffer.put(getByte());
        }

        tempBuffer.rewind();
        int result = tempBuffer.getInt();

        //System.out.println("get int: " + result);
        return result;
    }

    boolean hasRemaining() {
        //System.out.println("hasRemaining pageId: " + pageId + " numpages " + file.getNumPages());
        if (pageId < file.getNumPages() - 1) {
            return true;
        }

        if (pageId == file.getNumPages() - 1 && buffer.hasRemaining()) {
            if (buffer.position() < buffer.capacity() - 4) {
                int nextWordLength = buffer.getInt();
                buffer.position(buffer.position() - 4);
                return nextWordLength > 0;
            }
        }

        return false;
    }
}

