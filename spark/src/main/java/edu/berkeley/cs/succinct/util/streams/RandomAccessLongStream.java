package edu.berkeley.cs.succinct.util.streams;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * LongBuffer like wrapper for FSDataInputStream
 */
public class RandomAccessLongStream {

  private FSDataInputStream stream;
  private long startPos;
  private long limit;

  public RandomAccessLongStream(FSDataInputStream stream, long startPos, long limit)
    throws IOException {
    this.stream = stream;
    this.startPos = startPos;
    this.limit = limit;
    stream.seek(startPos);
  }

  public long get() throws IOException {
    if (stream.getPos() >= startPos + limit * 8) {
      throw new ArrayIndexOutOfBoundsException(
        "Stream out of bounds: startPos = " + startPos + " limit = " + limit);
    }
    return stream.readLong();
  }

  public long get(int index) throws IOException {
    if (index >= limit) {
      throw new ArrayIndexOutOfBoundsException(
        "Stream out of bounds: startPos = " + startPos + " limit = " + limit + " index = " + index);
    }
    long currentPos = stream.getPos();
    stream.seek(startPos + index * 8);
    long returnValue = stream.readLong();
    stream.seek(currentPos);
    return returnValue;
  }

  public long position() throws IOException {
    long pos = stream.getPos() - startPos;
    assert (pos % 8 == 0);
    return pos / 8;
  }

  public RandomAccessLongStream position(long pos) throws IOException {
    if (pos * 8 >= limit * 8) {
      throw new ArrayIndexOutOfBoundsException(
        "Stream out of bounds: startPos = " + startPos + " limit = " + limit + " pos = " + pos);
    }
    stream.seek(startPos + pos * 8);
    return this;
  }

  public RandomAccessLongStream rewind() throws Exception {
    return position(0);
  }

  public long limit() {
    return limit;
  }

  public void close() throws IOException {
    stream.close();
  }
}
