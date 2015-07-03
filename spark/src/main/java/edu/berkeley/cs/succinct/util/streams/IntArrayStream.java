package edu.berkeley.cs.succinct.util.streams;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * IntArray-like wrapper for FSDataInputStream
 */
public class IntArrayStream {
    int size;
    long startPos;
    FSDataInputStream stream;

    public IntArrayStream(FSDataInputStream stream, long startPos, int sizeInBytes) {
        this.stream = stream;
        this.startPos = startPos;
        this.size = sizeInBytes / 4;
    }

    public int get(int i) throws IOException {
        if (i < 0 || i >= size) {
            throw new ArrayIndexOutOfBoundsException("i = " + i + " size = " + size);
        }
        stream.seek(startPos + i * 4);
        return stream.readInt();
    }

    public long size() {
        return size;
    }

    public void close() throws IOException {
        stream.close();
    }
}
