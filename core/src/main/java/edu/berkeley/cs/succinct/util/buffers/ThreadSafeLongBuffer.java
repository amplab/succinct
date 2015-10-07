package edu.berkeley.cs.succinct.util.buffers;

import java.nio.Buffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class ThreadSafeLongBuffer {

  LongBufferLocal buf;

  public ThreadSafeLongBuffer(LongBuffer buf) {
    this.buf = new LongBufferLocal(buf);
  }

  public static ThreadSafeLongBuffer fromLongBuffer(LongBuffer buf) {
    if (buf == null)
      return null;
    return new ThreadSafeLongBuffer(buf);
  }

  public static ThreadSafeLongBuffer allocate(int capacity) {
    return new ThreadSafeLongBuffer(LongBuffer.allocate(capacity));
  }

  public static ThreadSafeLongBuffer wrap(long[] array) {
    return new ThreadSafeLongBuffer(LongBuffer.wrap(array));
  }

  public LongBuffer buffer() {
    return buf.get();
  }

  public LongBuffer slice() {
    return buf.get().slice();
  }

  public LongBuffer duplicate() {
    return buf.get().duplicate();
  }

  public LongBuffer asReadOnlyBuffer() {
    return buf.get().asReadOnlyBuffer();
  }

  public long get() {
    return buf.get().get();
  }

  public LongBuffer put(long l) {
    return buf.get().put(l);
  }

  public long get(int index) {
    return buf.get().get(index);
  }

  public LongBuffer put(int index, long l) {
    return buf.get().put(index, l);
  }

  public LongBuffer compact() {
    return buf.get().compact();
  }

  public boolean isReadOnly() {
    return buf.get().isReadOnly();
  }

  public boolean hasArray() {
    return buf.get().hasArray();
  }

  public Object array() {
    return buf.get().array();
  }

  public int arrayOffset() {
    return buf.get().arrayOffset();
  }

  public boolean isDirect() {
    return buf.get().isDirect();
  }

  public ByteOrder order() {
    return buf.get().order();
  }

  public Buffer position(int newPosition) {
    return buf.get().position(newPosition);
  }

  public int capacity() {
    return buf.get().capacity();
  }

  public int limit() {
    return buf.get().limit();
  }

  public Buffer limit(int newLimit) {
    return buf.get().limit(newLimit);
  }

  public Buffer rewind() {
    return buf.get().rewind();
  }

  public class LongBufferLocal extends ThreadLocal<LongBuffer> {
    private LongBuffer _src;

    public LongBufferLocal(LongBuffer src) {
      _src = (LongBuffer) src.rewind();
    }

    @Override protected synchronized LongBuffer initialValue() {
      return _src.duplicate();
    }
  }

}
