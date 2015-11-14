package edu.berkeley.cs.succinct.util.buffer;

import java.nio.*;

public class ThreadSafeByteBuffer {

  ByteBufferLocal buf;

  public ThreadSafeByteBuffer(ByteBuffer buf) {
    this.buf = new ByteBufferLocal(buf);
  }

  public static ThreadSafeByteBuffer fromByteBuffer(ByteBuffer buf) {
    if (buf == null)
      return null;
    return new ThreadSafeByteBuffer(buf);
  }

  public static ThreadSafeByteBuffer allocate(int capacity) {
    return new ThreadSafeByteBuffer(ByteBuffer.allocate(capacity));
  }

  public static ThreadSafeByteBuffer wrap(byte[] array) {
    return new ThreadSafeByteBuffer(ByteBuffer.wrap(array));
  }

  public ByteBuffer buffer() {
    return buf.get();
  }

  public ByteBuffer slice() {
    return buf.get().slice();
  }

  public ByteBuffer duplicate() {
    return buf.get().duplicate();
  }

  public ByteBuffer asReadOnlyBuffer() {
    return buf.get().asReadOnlyBuffer();
  }

  public byte get() {
    return buf.get().get();
  }

  public ByteBuffer put(byte b) {
    return buf.get().put(b);
  }

  public byte get(int index) {
    return buf.get().get(index);
  }

  public ByteBuffer put(int index, byte b) {
    return buf.get().put(index, b);
  }

  public ByteBuffer compact() {
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

  public char getChar() {
    return buf.get().getChar();
  }

  public ByteBuffer putChar(char value) {
    return buf.get().putChar(value);
  }

  public char getChar(int index) {
    return buf.get().getChar();
  }

  public ByteBuffer putChar(int index, char value) {
    return buf.get().putChar(index, value);
  }

  public CharBuffer asCharBuffer() {
    return buf.get().asCharBuffer();
  }

  public short getShort() {
    return buf.get().getShort();
  }

  public ByteBuffer putShort(short value) {
    return buf.get().putShort(value);
  }

  public short getShort(int index) {
    return buf.get().getShort(index);
  }

  public ByteBuffer putShort(int index, short value) {
    return buf.get().putShort(index, value);
  }

  public ShortBuffer asShortBuffer() {
    return buf.get().asShortBuffer();
  }

  public int getInt() {
    return buf.get().getInt();
  }

  public ByteBuffer putInt(int value) {
    return buf.get().putInt(value);
  }

  public int getInt(int index) {
    return buf.get().getInt();
  }

  public ByteBuffer putInt(int index, int value) {
    return buf.get().putInt(index, value);
  }

  public IntBuffer asIntBuffer() {
    return buf.get().asIntBuffer();
  }

  public long getLong() {
    return buf.get().getLong();
  }

  public ByteBuffer putLong(long value) {
    return buf.get().putLong(value);
  }

  public long getLong(int index) {
    return buf.get().getLong(index);
  }

  public ByteBuffer putLong(int index, long value) {
    return buf.get().putLong(index, value);
  }

  public LongBuffer asLongBuffer() {
    return buf.get().asLongBuffer();
  }

  public float getFloat() {
    return buf.get().getFloat();
  }

  public ByteBuffer putFloat(float value) {
    return buf.get().putFloat(value);
  }

  public float getFloat(int index) {
    return buf.get().getFloat(index);
  }

  public ByteBuffer putFloat(int index, float value) {
    return buf.get().putFloat(index, value);
  }

  public FloatBuffer asFloatBuffer() {
    return buf.get().asFloatBuffer();
  }

  public double getDouble() {
    return buf.get().getDouble();
  }

  public ByteBuffer putDouble(double value) {
    return buf.get().putDouble(value);
  }

  public double getDouble(int index) {
    return buf.get().getDouble(index);
  }

  public ByteBuffer putDouble(int index, double value) {
    return buf.get().putDouble(index, value);
  }

  public DoubleBuffer asDoubleBuffer() {
    return buf.get().asDoubleBuffer();
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

  public ByteBuffer order(ByteOrder order) {
    return buf.get().order(order);
  }

  public ByteOrder order() {
    return buf.get().order();
  }

  public Buffer rewind() {
    return buf.get().rewind();
  }

  public class ByteBufferLocal extends ThreadLocal<ByteBuffer> {
    private ByteBuffer _src;

    public ByteBufferLocal(ByteBuffer src) {
      _src = (ByteBuffer) src.rewind();
    }

    @Override protected synchronized ByteBuffer initialValue() {
      return _src.duplicate();
    }
  }
}
