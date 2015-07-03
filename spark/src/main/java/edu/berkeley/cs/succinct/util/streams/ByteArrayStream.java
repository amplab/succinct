package edu.berkeley.cs.succinct.util.streams;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * ByteArray-like wrapper for FSDataInputStream
 */
public class ByteArrayStream {
    int size;
    long startPos;
    FSDataInputStream stream;

    public ByteArrayStream(FSDataInputStream stream, long startPos, int sizeInBytes) {
        this.stream = stream;
        this.startPos = startPos;
        this.size = sizeInBytes;
    }

    public byte get(int i) throws IOException {
        if (i < 0 || i >= size) {
            throw new ArrayIndexOutOfBoundsException("i = " + i + " size = " + size);
        }
        stream.seek(startPos + i);
        return stream.readByte();
    }

    public long size() {
        return size;
    }

    public void close() throws IOException {
        stream.close();
    }
}
