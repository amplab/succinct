package edu.berkeley.cs.succinct.util.streams;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * IntArray-like wrapper for FSDataInputStream
 */
public class LongArrayStream {
    int size;
    long startPos;
    FSDataInputStream stream;

    public LongArrayStream(FSDataInputStream stream, long startPos, int sizeInBytes) {
        this.stream = stream;
        this.startPos = startPos;
        this.size = sizeInBytes / 8;
    }

    public long get(int i) throws IOException {
        if(i < 0 || i >= size) {
            throw new ArrayIndexOutOfBoundsException("i = " + i + " size = " + size);
        }
        stream.seek(startPos + i * 8);
        return stream.readLong();
    }

    public void close() throws IOException {
        stream.close();
    }

    public int size() {
        return size;
    }

}
