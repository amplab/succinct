package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.SuccinctRegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Source;
import edu.berkeley.cs.succinct.util.SuccinctConfiguration;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Range;
import edu.berkeley.cs.succinct.util.iterator.SearchIterator;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;

public class SuccinctFileBuffer extends SuccinctBuffer implements SuccinctFile {

  private static final long serialVersionUID = 5879363803993345049L;

  /**
   * Constructor to create SuccinctBuffer from byte array and file offset.
   *
   * @param input Input byte array.
   * @param conf  Succinct configuration.
   */
  public SuccinctFileBuffer(final char[] input, SuccinctConfiguration conf) {
    super(input, conf);
  }

  /**
   * Constructor to create SuccinctBuffer from byte array and file offset.
   *
   * @param input Input byte array.
   */
  public SuccinctFileBuffer(final char[] input) {
    super(input);
  }

  /**
   * Constructor to create SuccinctBuffer from byte array and file offset.
   *
   * @param input Input byte array.
   * @param conf  Succinct configuration.
   */
  public SuccinctFileBuffer(final byte[] input, SuccinctConfiguration conf) {
    super(input, conf);
  }

  /**
   * Constructor to create SuccinctBuffer from byte array and file offset.
   *
   * @param input Input byte array.
   */
  public SuccinctFileBuffer(final byte[] input) {
    super(input);
  }

  /**
   * Constructor to load the data from persisted Succinct data-structures.
   *
   * @param path        Path to load data from.
   * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
   */
  public SuccinctFileBuffer(String path, StorageMode storageMode) {
    super(path, storageMode);
  }

  /**
   * Constructor to load the data from a ByteBuffer.
   *
   * @param buf Input buffer to load the data from
   */
  public SuccinctFileBuffer(ByteBuffer buf) {
    super(buf);
  }

  /**
   * Default constructor.
   */
  public SuccinctFileBuffer() {
    super();
  }

  /**
   * Get the alphabet for the succinct file.
   *
   * @return The alphabet for the succinct file.
   */
  @Override public int[] getAlphabet() {
    return alphabet;
  }

  /**
   * Get the size of the uncompressed file.
   *
   * @return The size of the uncompressed file.
   */
  @Override public int getSize() {
    return getOriginalSize();
  }

  /**
   * Get the size of the Succinct compressed file.
   *
   * @return The size of the Succinct compressed file.
   */
  @Override public int getCompressedSize() {
    return getCoreSize();
  }

  /**
   * Get the character at index in file.
   *
   * @param i Index into file.
   * @return The character at the specified index.
   */
  @Override public char charAt(long i) {
    return (char) lookupC(lookupISA(i));
  }

  /**
   * Extract data of specified length from Succinct data structures at specified index.
   *
   * @param offset Index into original input to start extracting at.
   * @param len    Length of data to be extracted.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted data.
   */
  @Override public String extract(long offset, int len, ExtractContext ctx) {
    StringBuilder out = new StringBuilder(len);
    long s = lookupISA(offset);
    for (int k = 0; k < len && offset + k < getOriginalSize(); k++) {
      int nextChar = lookupC(s);
      if (nextChar < Character.MIN_VALUE || nextChar > Character.MAX_VALUE)
        break;
      out.append((char) nextChar);
      s = lookupNPA(s);
    }

    if (ctx != null)
      ctx.marker = s;

    return out.toString();
  }

  /**
   * Extract data of specified length from Succinct data structures at specified index.
   *
   * @param offset Index into original input to start extracting at.
   * @param length Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public String extract(long offset, int length) {
    return extract(offset, length, null);
  }

  /**
   * Extract data of specified length from Succinct data structures.
   *
   * @param ctx Extract context containing the end marker of previous extract.
   * @param len Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public String extract(ExtractContext ctx, int len) {
    StringBuilder out = new StringBuilder(len);
    for (int k = 0; k < len; k++) {
      int nextChar = lookupC(ctx.marker);
      if (nextChar < Character.MIN_VALUE || nextChar > Character.MAX_VALUE)
        break;
      out.append((char) nextChar);
      ctx.marker = lookupNPA(ctx.marker);
    }
    return out.toString();
  }

  /**
   * Extract data from Succinct data structures at specified index until specified delimiter.
   *
   * @param offset Index into original input to start extracting at.
   * @param delim  Delimiter at which to stop extracting.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted data.
   */
  @Override public String extractUntil(long offset, int delim, ExtractContext ctx) {
    StringBuilder out = new StringBuilder();

    long s = lookupISA(offset);
    do {
      int nextChar = lookupC(s);
      if (nextChar == delim || nextChar == SuccinctConstants.EOF)
        break;
      out.append((char) nextChar);
      s = lookupNPA(s);
    } while (true);

    if (ctx != null)
      ctx.marker = s;

    return out.toString();
  }

  /**
   * Extract data from Succinct data structures at specified index until specified delimiter.
   *
   * @param offset Index into original input to start extracting at.
   * @param delim  Delimiter at which to stop extracting.
   * @return Extracted data.
   */
  @Override public String extractUntil(long offset, int delim) {
    return extractUntil(offset, delim, null);
  }

  /**
   * Extract data from Succinct data structures until specified delimiter.
   *
   * @param ctx   Extract context containing the end marker of previous extract.
   * @param delim Delimiter at which to stop extracting.
   * @return Extracted data.
   */
  @Override public String extractUntil(ExtractContext ctx, int delim) {
    StringBuilder out = new StringBuilder();

    do {
      int nextChar = lookupC(ctx.marker);
      if (nextChar == delim || nextChar == SuccinctConstants.EOF)
        break;
      out.append((char) nextChar);
      ctx.marker = lookupNPA(ctx.marker);
    } while (true);

    return out.toString();
  }

  /**
   * Extract data of specified length from Succinct data structures at specified index.
   *
   * @param offset Index into original input to start extracting at.
   * @param len    Length of data to be extracted.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted data.
   */
  @Override public byte[] extractBytes(long offset, int len, ExtractContext ctx) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(len);
    long s = lookupISA(offset);
    for (int k = 0; k < len && offset + k < getOriginalSize(); k++) {
      int nextByte = lookupC(s);
      if (nextByte < Byte.MIN_VALUE || nextByte > Byte.MAX_VALUE)
        break;
      out.write(nextByte);
      s = lookupNPA(s);
    }

    if (ctx != null)
      ctx.marker = s;

    return out.toByteArray();
  }

  /**
   * Extract data of specified length from Succinct data structures at specified index.
   *
   * @param offset Index into original input to start extracting at.
   * @param length Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public byte[] extractBytes(long offset, int length) {
    return extractBytes(offset, length, null);
  }

  /**
   * Extract data of specified length from Succinct data structures.
   *
   * @param ctx Extract context containing the end marker of previous extract.
   * @param len Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public byte[] extractBytes(ExtractContext ctx, int len) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(len);
    for (int k = 0; k < len; k++) {
      int nextByte = lookupC(ctx.marker);
      if (nextByte < Byte.MIN_VALUE || nextByte > Byte.MAX_VALUE)
        break;
      out.write(nextByte);
      ctx.marker = lookupNPA(ctx.marker);
    }

    return out.toByteArray();
  }

  /**
   * Extract data from Succinct data structures at specified index until specified delimiter.
   *
   * @param offset Index into original input to start extracting at.
   * @param delim  Delimiter at which to stop extracting.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted data.
   */
  @Override public byte[] extractBytesUntil(long offset, int delim, ExtractContext ctx) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long s = lookupISA(offset);
    do {
      int nextByte = lookupC(s);
      if (nextByte == delim || nextByte == SuccinctConstants.EOF)
        break;
      out.write(nextByte);
      s = lookupNPA(s);
    } while (true);

    if (ctx != null)
      ctx.marker = s;

    return out.toByteArray();
  }

  /**
   * Extract data from Succinct data structures at specified index until specified delimiter.
   *
   * @param offset Index into original input to start extracting at.
   * @param delim  Delimiter at which to stop extracting.
   * @return Extracted data.
   */
  @Override public byte[] extractBytesUntil(long offset, int delim) {
    return extractBytesUntil(offset, delim, null);
  }

  /**
   * Extract data from Succinct data structures until specified delimiter.
   *
   * @param ctx   Extract context containing the end marker of previous extract.
   * @param delim Delimiter at which to stop extracting.
   * @return Extracted data.
   */
  @Override public byte[] extractBytesUntil(ExtractContext ctx, int delim) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    do {
      int nextByte = lookupC(ctx.marker);
      if (nextByte == delim || nextByte == SuccinctConstants.EOF)
        break;
      out.write(nextByte);
      ctx.marker = lookupNPA(ctx.marker);
    } while (true);

    return out.toByteArray();
  }

  /**
   * Extract short integer at specified offset.
   *
   * @param offset Offset into the original input to start extracting at.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted short integer.
   */
  @Override public short extractShort(int offset, ExtractContext ctx) {
    long s = lookupISA(offset);
    int byte0 = lookupC(s);

    s = lookupNPA(s);
    int byte1 = lookupC(s);

    if (ctx != null)
      ctx.marker = lookupNPA(s);

    return (short) ((byte0 << 8) | (byte1 & 0xFF));
  }

  /**
   * Extract short integer at specified offset.
   *
   * @param offset Offset into the original input to start extracting at.
   * @return Extracted short integer.
   */
  @Override public short extractShort(int offset) {
    return extractShort(offset, null);
  }

  /**
   * Extract short integer at specified offset.
   *
   * @param ctx Extract context containing the end marker of previous extract.
   * @return Extracted short integer.
   */
  @Override public short extractShort(ExtractContext ctx) {
    int byte0 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte1 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);

    return (short) ((byte0 << 8) | (byte1 & 0xFF));
  }

  /**
   * Extract integer at specified offset.
   *
   * @param offset Offset into the original input to start extracting at.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted integer.
   */
  @Override public int extractInt(int offset, ExtractContext ctx) {
    long s = lookupISA(offset);
    int byte0 = lookupC(s);

    s = lookupNPA(s);
    int byte1 = lookupC(s);

    s = lookupNPA(s);
    int byte2 = lookupC(s);

    s = lookupNPA(s);
    int byte3 = lookupC(s);

    if (ctx != null)
      ctx.marker = lookupNPA(s);

    return (byte0 << 24) | ((byte1 & 0xFF) << 16) | ((byte2 & 0xFF) << 8) | (byte3 & 0xFF);
  }

  /**
   * Extract integer at specified offset.
   *
   * @param offset Offset into the original input to start extracting at.
   * @return Extracted integer.
   */
  @Override public int extractInt(int offset) {
    return extractInt(offset, null);
  }

  /**
   * Extract integer at specified offset.
   *
   * @param ctx Extract context containing the end marker of previous extract.
   * @return Extracted integer.
   */
  @Override public int extractInt(ExtractContext ctx) {
    int byte0 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte1 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte2 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte3 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);

    return (byte0 << 24) | ((byte1 & 0xFF) << 16) | ((byte2 & 0xFF) << 8) | (byte3 & 0xFF);
  }

  /**
   * Extract long integer at specified offset.
   *
   * @param offset Offset into the original input to start extracting at.
   * @param ctx    Extract context to be populated with end marker of extract.
   * @return Extracted long integer.
   */
  @Override public long extractLong(int offset, ExtractContext ctx) {
    long s = lookupISA(offset);
    int byte0 = lookupC(s);

    s = lookupNPA(s);
    int byte1 = lookupC(s);

    s = lookupNPA(s);
    int byte2 = lookupC(s);

    s = lookupNPA(s);
    int byte3 = lookupC(s);

    s = lookupNPA(s);
    int byte4 = lookupC(s);

    s = lookupNPA(s);
    int byte5 = lookupC(s);

    s = lookupNPA(s);
    int byte6 = lookupC(s);

    s = lookupNPA(s);
    int byte7 = lookupC(s);

    if (ctx != null)
      ctx.marker = lookupNPA(s);

    return ((long) byte0 << 56) | ((long) (byte1 & 0xFF) << 48) | ((long) (byte2 & 0xFF) << 40) | (
      (long) (byte3 & 0xFF) << 32) | ((long) (byte4 & 0xFF) << 24) | ((byte5 & 0xFF) << 16) | (
      (byte6 & 0xFF) << 8) | ((byte7 & 0xFF));
  }

  /**
   * Extract long integer at specified offset.
   *
   * @param offset Offset into the original input to start extracting at.
   * @return Extracted long integer.
   */
  @Override public long extractLong(int offset) {
    return extractLong(offset, null);
  }

  /**
   * Extract long integer at specified offset.
   *
   * @param ctx Extract context containing the end marker of previous extract.
   * @return Extracted long integer.
   */
  @Override public long extractLong(ExtractContext ctx) {
    int byte0 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte1 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte2 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte3 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte4 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte5 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte6 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);
    int byte7 = lookupC(ctx.marker);

    ctx.marker = lookupNPA(ctx.marker);

    return ((long) byte0 << 56) | ((long) (byte1 & 0xFF) << 48) | ((long) (byte2 & 0xFF) << 40) | (
      (long) (byte3 & 0xFF) << 32) | ((long) (byte4 & 0xFF) << 24) | ((byte5 & 0xFF) << 16) | (
      (byte6 & 0xFF) << 8) | ((byte7 & 0xFF));
  }

  /**
   * Perform a range search to obtain SA range between two given queries.
   *
   * @param buf1 The beginning of the range.
   * @param buf2 The end of the range.
   * @return The range into SA.
   */
  @Override public Range rangeSearch(char[] buf1, char[] buf2) {
    return new Range(fwdSearch(buf1).begin(), fwdSearch(buf2).end());
  }

  /**
   * Perform a range search to obtain SA range between two given queries.
   *
   * @param buf1 The beginning of the range.
   * @param buf2 The end of the range.
   * @return The range into SA.
   */
  @Override public Range rangeSearch(byte[] buf1, byte[] buf2) {
    return new Range(fwdSearch(buf1).begin(), fwdSearch(buf2).end());
  }

  /**
   * Perform a range search to obtain SA range between two given queries.
   *
   * @param buf1 The beginning of the range.
   * @param buf2 The end of the range.
   * @return The range into SA.
   */
  @Override public Range rangeSearch(Source buf1, Source buf2) {
    return new Range(fwdSearch(buf1).begin(), fwdSearch(buf2).end());
  }

  /**
   * Perform backward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range bwdSearch(Source buf) {
    Range range = new Range(0L, -1L);
    int m = buf.length();
    long c1, c2;

    int pos = findCharacter(buf.get(m - 1));
    if (pos >= 0) {
      range.first = columnoffsets.get(pos);
      range.second =
        ((pos + 1) == getAlphabetSize() ? getOriginalSize() : columnoffsets.get(pos + 1)) - 1;
    } else {
      return new Range(0L, -1L);
    }

    for (int i = m - 2; i >= 0; i--) {
      pos = findCharacter(buf.get(i));
      if (pos >= 0) {
        c1 = columnoffsets.get(pos);
        c2 = ((pos + 1) == getAlphabetSize() ? getOriginalSize() : columnoffsets.get(pos + 1)) - 1;
      } else {
        return new Range(0L, -1L);
      }

      if (c1 > c2) {
        return new Range(0L, -1L);
      }

      range.first = binSearchNPA(range.first, c1, c2, false);
      range.second = binSearchNPA(range.second, c1, c2, true);

      if (range.first > range.second) {
        return new Range(0L, -1L);
      }
    }

    return range;
  }

  /**
   * Perform backward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range bwdSearch(final byte[] buf) {
    return bwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    });
  }

  /**
   * Perform backward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range bwdSearch(final char[] buf) {
    return bwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    });
  }

  /**
   * Continue backward search on query to obtain SA range.
   *
   * @param buf   Input query.
   * @param range Range to start from.
   * @return Range into SA.
   */
  @Override public Range continueBwdSearch(Source buf, Range range) {
    if (range.empty()) {
      return range;
    }

    Range newRange = new Range(range.first, range.second);
    int m = buf.length();
    long c1, c2;

    for (int i = m - 1; i >= 0; i--) {
      int pos = findCharacter(buf.get(i));
      if (pos >= 0) {
        c1 = columnoffsets.get(pos);
        c2 = ((pos + 1) == getAlphabetSize() ? getOriginalSize() : columnoffsets.get(pos + 1)) - 1;
      } else {
        return new Range(0L, -1L);
      }

      if (c1 > c2) {
        return new Range(0L, -1L);
      }

      newRange.first = binSearchNPA(newRange.first, c1, c2, false);
      newRange.second = binSearchNPA(newRange.second, c1, c2, true);

      if (newRange.first > newRange.second) {
        return new Range(0L, -1L);
      }
    }
    return newRange;
  }

  /**
   * Continue backward search on query to obtain SA range.
   *
   * @param buf   Input query.
   * @param range Range to start from.
   * @return Range into SA.
   */
  @Override public Range continueBwdSearch(final byte[] buf, Range range) {
    return continueBwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, range);
  }

  /**
   * Continue backward search on query to obtain SA range.
   *
   * @param buf   Input query.
   * @param range Range to start from.
   * @return Range into SA.
   */
  @Override public Range continueBwdSearch(final char[] buf, Range range) {
    return continueBwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, range);
  }

  /**
   * Compare entire buffer with input starting at specified index.
   *
   * @param buf The buffer to compare with.
   * @param i   The index into input.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(Source buf, int i) {
    int j = 0;

    do {
      int c = lookupC(i);
      int b = buf.get(j);
      if (b < c) {
        return -1;
      } else if (b > c) {
        return 1;
      }
      i = (int) lookupNPA(i);
      j++;
    } while (j < buf.length());

    return 0;
  }

  /**
   * Compare entire buffer with input starting at specified index.
   *
   * @param buf The buffer to compare with.
   * @param i   The index into input.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(final byte[] buf, int i) {
    return compare(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, i);
  }

  /**
   * Compare entire buffer with input starting at specified index.
   *
   * @param buf The buffer to compare with.
   * @param i   The index into input.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(final char[] buf, int i) {
    return compare(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, i);
  }

  /**
   * Compare entire buffer with input starting at specified index and offset
   * into buffer.
   *
   * @param buf    The buffer to compare with.
   * @param i      The index into input.
   * @param offset Offset into buffer.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(Source buf, int i, int offset) {
    int j = 0;

    while (offset != 0) {
      i = (int) lookupNPA(i);
      offset--;
    }

    do {
      int c = lookupC(i);
      int b = buf.get(j);
      if (b < c) {
        return -1;
      } else if (b > c) {
        return 1;
      }
      i = (int) lookupNPA(i);
      j++;
    } while (j < buf.length());

    return 0;
  }

  /**
   * Compare entire buffer with input starting at specified index and offset
   * into buffer.
   *
   * @param buf    The buffer to compare with.
   * @param i      The index into input.
   * @param offset Offset into buffer.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(final byte[] buf, int i, int offset) {
    return compare(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, i, offset);
  }

  /**
   * Compare entire buffer with input starting at specified index and offset
   * into buffer.
   *
   * @param buf    The buffer to compare with.
   * @param i      The index into input.
   * @param offset Offset into buffer.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(final char[] buf, int i, int offset) {
    return compare(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, i, offset);
  }

  /**
   * Perform forward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range fwdSearch(Source buf) {
    int st = getOriginalSize() - 1;
    int sp = 0;
    int s;
    while (sp < st) {
      s = (sp + st) / 2;
      if (compare(buf, s) > 0) {
        sp = s + 1;
      } else {
        st = s;
      }
    }

    int et = getOriginalSize() - 1;
    int ep = sp - 1;
    int e;
    while (ep < et) {
      e = (int) Math.ceil((double) (ep + et) / 2);
      if (compare(buf, e) == 0) {
        ep = e;
      } else {
        et = e - 1;
      }
    }

    return new Range(sp, ep);
  }

  /**
   * Perform forward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range fwdSearch(final byte[] buf) {
    return fwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    });
  }

  /**
   * Perform forward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range fwdSearch(final char[] buf) {
    return fwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    });
  }

  /**
   * Continue forward search on query to obtain SA range.
   *
   * @param buf    Input query.
   * @param range  Range to start from.
   * @param offset Offset into input query.
   * @return Range into SA.
   */
  @Override public Range continueFwdSearch(Source buf, Range range, int offset) {

    if (buf.length() == 0 || range.empty()) {
      return range;
    }

    int st = (int) range.second;
    int sp = (int) range.first;
    int s;
    while (sp < st) {
      s = (sp + st) / 2;
      if (compare(buf, s, offset) > 0) {
        sp = sp + 1;
      } else {
        st = s;
      }
    }

    int et = (int) range.second;
    int ep = sp - 1;
    int e;
    while (ep < et) {
      e = (int) Math.ceil((double) (ep + et) / 2);
      if (compare(buf, e, offset) == 0) {
        ep = e;
      } else {
        et = e - 1;
      }
    }

    return new Range(sp, ep);
  }

  /**
   * Continue forward search on query to obtain SA range.
   *
   * @param buf    Input query.
   * @param range  Range to start from.
   * @param offset Offset into input query.
   * @return Range into SA.
   */
  @Override public Range continueFwdSearch(final byte[] buf, Range range, int offset) {
    return continueFwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, range, offset);
  }

  /**
   * Continue forward search on query to obtain SA range.
   *
   * @param buf    Input query.
   * @param range  Range to start from.
   * @param offset Offset into input query.
   * @return Range into SA.
   */
  @Override public Range continueFwdSearch(final char[] buf, Range range, int offset) {
    return continueFwdSearch(new Source() {
      @Override public int length() {
        return buf.length;
      }

      @Override public int get(int i) {
        return buf[i];
      }
    }, range, offset);
  }

  /**
   * Get count of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return Count of occurrences.
   */
  @Override public long count(Source query) {
    Range range = bwdSearch(query);
    return range.second - range.first + 1;
  }

  /**
   * Converts Succinct index (i.e., Compressed Suffix Array index) to file offset.
   *
   * @param i Compressed Suffix Array index.
   * @return File offset.
   */
  @Override public Long succinctIndexToOffset(long i) {
    return lookupSA(i);
  }

  /**
   * Get count of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return Count of occurrences.
   */
  @Override public long count(final byte[] query) {
    return count(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Get count of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return Count of occurrences.
   */
  @Override public long count(final char[] query) {
    return count(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  @Override public Iterator<Long> searchIterator(Source query) {
    Range range = bwdSearch(query);
    return new SearchIterator(this, range);
  }

  /**
   * Search for locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Iterator<Long> searchIterator(final byte[] query) {
    return searchIterator(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Search for locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Iterator<Long> searchIterator(final char[] query) {
    return searchIterator(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Translate range into SA to recordIds in file.
   *
   * @param range Range into SA.
   * @return Offsets corresponding to recordIds.
   */
  @Override public Long[] rangeToOffsets(Range range) {
    if (range.empty()) {
      return new Long[0];
    }

    Long[] offsets = new Long[(int) range.size()];
    for (long i = 0; i < range.size(); i++) {
      offsets[((int) i)] = lookupSA(range.begin() + i);
    }

    return offsets;
  }

  /**
   * Get all locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Long[] search(Source query) {
    return rangeToOffsets(bwdSearch(query));
  }

  /**
   * Get all locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Long[] search(final byte[] query) {
    return search(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Get all locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Long[] search(final char[] query) {
    return search(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Check if the two offsets belong to the same record. This is always true for the
   * SuccinctFileBuffer.
   *
   * @param firstOffset  The first offset.
   * @param secondOffset The second offset.
   * @return True if the two offsets belong to the same record, false otherwise.
   */
  @Override public boolean sameRecord(long firstOffset, long secondOffset) {
    return true;
  }

  /**
   * Performs regular expression search for an input expression using Succinct data-structures.
   *
   * @param query Regular expression pattern to be matched. (UTF-8 encoded)
   * @return All locations and lengths of matching patterns in original input.
   * @throws RegExParsingException Parse exception on the query
   */
  @Override public Set<RegExMatch> regexSearch(String query) throws RegExParsingException {
    return new SuccinctRegEx(this, query).compute();
  }
}
