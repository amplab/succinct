package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.SuccinctRegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
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
   */
  public SuccinctFileBuffer(byte[] input) {
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
  @Override public byte[] getAlphabet() {
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

  @Override public int getSuccinctFileSize() {
    return super.getSuccinctSize();
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
   * @param length Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public byte[] extract(long offset, int length) {

    byte[] buf = new byte[length];
    long s = lookupISA(offset);
    for (int k = 0; k < length && k < getOriginalSize(); k++) {
      buf[k] = lookupC(s);
      s = lookupNPA(s);
    }

    return buf;
  }

  /**
   * Extract data from Succinct data structures at specified index until specified delimiter.
   *
   * @param offset Index into original input to start extracting at.
   * @param delim  Delimiter at which to stop extracting.
   * @return Extracted data.
   */
  @Override public byte[] extractUntil(long offset, byte delim) {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long s = lookupISA(offset);
    do {
      byte nextByte = lookupC(s);
      if (nextByte == delim || nextByte == SuccinctCore.EOF)
        break;
      out.write(nextByte);
      s = lookupNPA(s);
    } while (true);

    return out.toByteArray();
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
   * Perform backward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range bwdSearch(byte[] buf) {
    Range range = new Range(0L, -1L);
    int m = buf.length;
    long c1, c2;

    if (alphabetMap.containsKey(buf[m - 1])) {
      range.first = alphabetMap.get(buf[m - 1]).first;
      byte nextByte = alphabetMap.get(buf[m - 1]).second + 1 == getAlphabetSize() ?
        SuccinctCore.EOA :
        alphabet[alphabetMap.get(buf[m - 1]).second + 1];
      range.second = alphabetMap.get(nextByte).first - 1;
    } else {
      return new Range(0L, -1L);
    }

    for (int i = m - 2; i >= 0; i--) {
      if (alphabetMap.containsKey(buf[i])) {
        c1 = alphabetMap.get(buf[i]).first;
        byte nextByte = alphabetMap.get(buf[i]).second + 1 == getAlphabetSize() ?
          SuccinctCore.EOA :
          alphabet[alphabetMap.get(buf[i]).second + 1];
        c2 = alphabetMap.get(nextByte).first - 1;
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
   * Continue backward search on query to obtain SA range.
   *
   * @param buf   Input query.
   * @param range Range to start from.
   * @return Range into SA.
   */
  @Override public Range continueBwdSearch(byte[] buf, Range range) {
    if (range.empty()) {
      return range;
    }

    Range newRange = new Range(range.first, range.second);
    int m = buf.length;
    long c1, c2;

    for (int i = m - 1; i >= 0; i--) {
      if (alphabetMap.containsKey(buf[i])) {
        c1 = alphabetMap.get(buf[i]).first;
        byte nextByte = alphabetMap.get(buf[i]).second + 1 == getAlphabetSize() ?
          SuccinctCore.EOA :
          alphabet[alphabetMap.get(buf[i]).second + 1];
        c2 = alphabetMap.get(nextByte).first - 1;
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
   * Compare entire buffer with input starting at specified index.
   *
   * @param buf The buffer to compare with.
   * @param i   The index into input.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(byte[] buf, int i) {
    int j = 0;

    do {
      byte c = lookupC(i);
      if (buf[j] < c) {
        return -1;
      } else if (buf[j] > c) {
        return 1;
      }
      i = (int) lookupNPA(i);
      j++;
    } while (j < buf.length);

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
  @Override public int compare(byte[] buf, int i, int offset) {
    int j = 0;

    while (offset != 0) {
      i = (int) lookupNPA(i);
      offset--;
    }

    do {
      byte c = lookupC(i);
      if (buf[j] < c) {
        return -1;
      } else if (buf[j] > c) {
        return 1;
      }
      i = (int) lookupNPA(i);
      j++;
    } while (j < buf.length);

    return 0;
  }

  /**
   * Perform forward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range fwdSearch(byte[] buf) {
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
   * Continue forward search on query to obtain SA range.
   *
   * @param buf    Input query.
   * @param range  Range to start from.
   * @param offset Offset into input query.
   * @return Range into SA.
   */
  @Override public Range continueFwdSearch(byte[] buf, Range range, int offset) {

    if (buf.length == 0 || range.empty()) {
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
   * Get count of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return Count of occurrences.
   */
  @Override public long count(byte[] query) {
    Range range = bwdSearch(query);
    return range.second - range.first + 1;
  }

  /**
   * Translate range into SA to offsets in file.
   *
   * @param range Range into SA.
   * @return Offsets corresponding to offsets.
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

  @Override public Iterator<Long> searchIterator(byte[] query) {
    Range range = bwdSearch(query);
    return new SearchIterator(this, range);
  }

  /**
   * Get all locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Long[] search(byte[] query) {
    return rangeToOffsets(bwdSearch(query));
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
   * @throws RegExParsingException
   */
  @Override public Set<RegExMatch> regexSearch(String query) throws RegExParsingException {
    return new SuccinctRegEx(this, query).compute();
  }
}
