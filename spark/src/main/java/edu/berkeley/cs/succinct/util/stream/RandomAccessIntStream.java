package edu.berkeley.cs.succinct.util.stream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * IntBuffer like wrapper for FSDataInputStream
 */
public class RandomAccessIntStream {

  private FSDataInputStream stream;
  private long startPos;
  private long limit;

  public RandomAccessIntStream(FSDataInputStream stream, long startPos, long limit)
    throws IOException {
    this.stream = stream;
    this.startPos = startPos;
    this.limit = limit;
    stream.seek(startPos);
  }

  public int get() throws IOException {
    if (stream.getPos() >= startPos + limit * 4) {
      throw new ArrayIndexOutOfBoundsException(
        "Stream out of bounds: startPos = " + startPos + " limit = " + limit + " currentPos = "
          + stream.getPos());
    }
    return stream.readInt();
  }

  public int get(int index) throws IOException {
    if (index >= limit) {
      throw new ArrayIndexOutOfBoundsException(
        "Stream out of bounds: startPos = " + startPos + " limit = " + limit + " index = " + index);
    }
    long currentPos = stream.getPos();
    stream.seek(startPos + index * 4);
    int returnValue = stream.readInt();
    stream.seek(currentPos);
    return returnValue;
  }

  public long position() throws IOException {
    long pos = stream.getPos() - startPos;
    assert (pos % 4 == 0);
    return pos / 4;
  }

  public RandomAccessIntStream position(long pos) throws IOException {
    if (pos >= limit) {
      throw new ArrayIndexOutOfBoundsException(
        "Stream out of bounds: startPos = " + startPos + " limit = " + limit + " pos = " + pos);
    }
    stream.seek(startPos + pos * 4);
    return this;
  }

  public RandomAccessIntStream rewind() throws IOException {
    return position(0);
  }

  public void close() throws IOException {
    stream.close();
  }
}
