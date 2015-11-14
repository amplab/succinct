package edu.berkeley.cs.succinct.util.buffer;

import java.nio.Buffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

public class ThreadSafeIntBuffer {

  IntBufferLocal buf;

  public ThreadSafeIntBuffer(IntBuffer buf) {
    this.buf = new IntBufferLocal(buf);
  }

  public static ThreadSafeIntBuffer fromIntBuffer(IntBuffer buf) {
    if (buf == null)
      return null;
    return new ThreadSafeIntBuffer(buf);
  }

  public static ThreadSafeIntBuffer allocate(int capacity) {
    return new ThreadSafeIntBuffer(IntBuffer.allocate(capacity));
  }

  public static ThreadSafeIntBuffer wrap(int[] array) {
    return new ThreadSafeIntBuffer(IntBuffer.wrap(array));
  }

  public IntBuffer buffer() {
    return buf.get();
  }

  public IntBuffer slice() {
    return buf.get().slice();
  }

  public IntBuffer duplicate() {
    return buf.get().duplicate();
  }

  public IntBuffer asReadOnlyBuffer() {
    return buf.get().asReadOnlyBuffer();
  }

  public int get() {
    return buf.get().get();
  }

  public IntBuffer put(int i) {
    return buf.get().put(i);
  }

  public int get(int index) {
    return buf.get().get(index);
  }

  public IntBuffer put(int index, int i) {
    return buf.get().put(index, i);
  }

  public IntBuffer compact() {
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

  public class IntBufferLocal extends ThreadLocal<IntBuffer> {
    private IntBuffer _src;

    public IntBufferLocal(IntBuffer src) {
      _src = (IntBuffer) src.rewind();
    }

    @Override protected synchronized IntBuffer initialValue() {
      return _src.duplicate();
    }
  }
}
