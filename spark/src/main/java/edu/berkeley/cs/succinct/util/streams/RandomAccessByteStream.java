package edu.berkeley.cs.succinct.util.streams;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 *
 * ByteBuffer like wrapper for FSDataInputStream
 */

public class RandomAccessByteStream {
    private FSDataInputStream stream;
    private long startPos;
    private long limit;

    public RandomAccessByteStream(FSDataInputStream stream, long startPos, long limit) throws IOException {
        this.stream = stream;
        this.startPos = startPos;
        this.limit = limit;
        stream.seek(startPos);
    }

    public byte get() throws IOException {
        if(stream.getPos() >= startPos + limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit);
        }
        return stream.readByte();
    }

    public byte get(int index) throws IOException {
        if(index >= limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit + " index = " + index);
        }
        long currentPos = stream.getPos();
        stream.seek(startPos + index);
        byte returnValue = stream.readByte();
        stream.seek(currentPos);
        return returnValue;
    }

    public short getShort() throws IOException {
        if(stream.getPos() >= startPos + limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit);
        }
        return stream.readShort();
    }

    public short getShort(int index) throws IOException {
        if(index >= limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit + " index = " + index);
        }
        long currentPos = stream.getPos();
        stream.seek(startPos + index);
        short returnValue = stream.readShort();
        stream.seek(currentPos);
        return returnValue;
    }

    public int getInt() throws IOException {
        if(stream.getPos() >= startPos + limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit + " currentPos = " + stream.getPos());
        }
        return stream.readInt();
    }

    public int getInt(int index) throws IOException {
        if(index >= limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit + " index = " + index);
        }
        long currentPos = stream.getPos();
        stream.seek(startPos + index);
        int returnValue = stream.readInt();
        stream.seek(currentPos);
        return returnValue;
    }

    public long getLong() throws IOException {
        if(stream.getPos() >= startPos + limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit);
        }
        return stream.readLong();
    }

    public long getLong(int index) throws IOException {
        if(index >= limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit + " index = " + index);
        }
        long currentPos = stream.getPos();
        stream.seek(startPos + index);
        long returnValue = stream.readLong();
        stream.seek(currentPos);
        return returnValue;
    }

    public long position() throws IOException {
        return stream.getPos() - startPos;
    }

    public long absolutePosition() throws IOException {
        return stream.getPos();
    }

    public RandomAccessByteStream absolutePosition(long pos) throws IOException {
        stream.seek(pos);
        return this;
    }

    public RandomAccessByteStream position(long pos) throws IOException {
        if(pos >= limit) {
            throw new ArrayIndexOutOfBoundsException("Stream out of bounds: startPos = " + startPos
                    + " limit = " + limit + " position = " + pos);
        }
        stream.seek(startPos + pos);
        return this;
    }

    public RandomAccessIntStream asIntStream() throws IOException {
        return new RandomAccessIntStream(stream, stream.getPos(), (limit - (stream.getPos() - startPos)) / 4);
    }

    public RandomAccessLongStream asLongStream() throws IOException {
        return new RandomAccessLongStream(stream, stream.getPos(), (limit - (stream.getPos() - startPos)) / 8);
    }

    public RandomAccessByteStream rewind() throws IOException {
        return position(0);
    }

    public void close() throws IOException {
        stream.close();
    }
}
