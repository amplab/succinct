package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegexMatch;
import edu.berkeley.cs.succinct.regex.executor.RegExExecutor;
import edu.berkeley.cs.succinct.regex.parser.RegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParser;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.regex.planner.NaiveRegExPlanner;
import edu.berkeley.cs.succinct.regex.planner.RegExPlanner;
import edu.berkeley.cs.succinct.util.Range;
import edu.berkeley.cs.succinct.util.streams.SerializedOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SuccinctFileStream extends SuccinctStream implements SuccinctFile {

  protected transient long fileOffset;
  protected transient long endOfFileStream;

  /**
   * Constructor to map a file containing Succinct data structures via streams.
   *
   * @param filePath Path of the file.
   * @param conf     Configuration for the filesystem.
   * @throws IOException
   */
  public SuccinctFileStream(Path filePath, Configuration conf) throws IOException {
    super(filePath, conf);
    FSDataInputStream is = getStream(filePath);
    is.seek(endOfCoreStream);
    fileOffset = is.readLong();
    endOfFileStream = is.getPos();
    is.close();
  }

  /**
   * Constructor to map a file containing Succinct data structures via streams
   *
   * @param filePath Path of the file.
   * @throws IOException
   */
  public SuccinctFileStream(Path filePath) throws IOException {
    this(filePath, new Configuration());
  }

  /**
   * Get beginning offset for the file chunk.
   *
   * @return The beginning offset for the file chunk.
   */
  public long getFileOffset() {
    return fileOffset;
  }

  /**
   * Get offset range for the file chunk.
   *
   * @return The offset range for the file chunk.
   */
  public Range getFileRange() {
    return new Range(fileOffset, fileOffset + getOriginalSize() - 2);
  }

  public char partitionCharAt(long i) {
    try {
      return (char) alphabet.get(
        SerializedOperations.ArrayOps.getRank1(coloffsets, 0, getSigmaSize(), lookupISA(i)) - 1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Extract data of specified length from Succinct data structures at specified index.
   *
   * @param offset Index into original input to start extracting at.
   * @param len    Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public byte[] extract(long offset, int len) {
    byte[] buf = new byte[len];
    long s;

    try {
      long chunkOffset = offset - fileOffset;
      s = lookupISA(chunkOffset);
      for (int k = 0; k < len && k < getOriginalSize(); k++) {
        buf[k] = alphabet
          .get(SerializedOperations.ArrayOps.getRank1(coloffsets, 0, getSigmaSize(), s) - 1);
        s = lookupNPA(s);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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
    String strBuf = "";
    long s;

    try {
      long chunkOffset = offset - fileOffset;
      s = lookupISA(chunkOffset);
      char nextChar;
      do {
        nextChar = (char) alphabet
          .get(SerializedOperations.ArrayOps.getRank1(coloffsets, 0, getSigmaSize(), s) - 1);
        if (nextChar == delim || nextChar == 1)
          break;
        strBuf += nextChar;
        s = lookupNPA(s);
      } while (true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return strBuf.getBytes();
  }

  /**
   * Binary Search for a value withing NPA.
   *
   * @param val      Value to be searched.
   * @param startIdx Starting index into NPA.
   * @param endIdx   Ending index into NPA.
   * @param flag     Whether to search for left or the right boundary.
   * @return Search result as an index into the NPA.
   */
  @Override public long binSearchNPA(long val, long startIdx, long endIdx, boolean flag) {
    long sp = startIdx;
    long ep = endIdx;
    long m;

    while (sp <= ep) {
      m = (sp + ep) / 2;

      long npaVal;
      npaVal = lookupNPA(m);

      if (npaVal == val) {
        return m;
      } else if (val < npaVal) {
        ep = m - 1;
      } else {
        sp = m + 1;
      }
    }

    return flag ? ep : sp;
  }

  /**
   * Get range of SA positions using Backward search
   *
   * @param buf Input query to be searched.
   * @return Range of indices into the SA.
   */
  @Override public Range getRange(byte[] buf) {
    Range range = new Range(0L, -1L);
    int m = buf.length;
    long c1, c2;

    try {
      if (alphabetMap.containsKey(buf[m - 1])) {
        range.first = alphabetMap.get(buf[m - 1]).first;
        range.second =
          alphabetMap.get((alphabet.get(alphabetMap.get(buf[m - 1]).second + 1))).first - 1;
      } else {
        return range;
      }

      for (int i = m - 2; i >= 0; i--) {
        if (alphabetMap.containsKey(buf[i])) {
          c1 = alphabetMap.get(buf[i]).first;
          c2 = alphabetMap.get((alphabet.get(alphabetMap.get(buf[i]).second + 1))).first - 1;
        } else {
          return range;
        }
        range.first = binSearchNPA(range.first, c1, c2, false);
        range.second = binSearchNPA(range.second, c1, c2, true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return range;
  }

  /**
   * Get count of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return Count of occurrences.
   */
  @Override public long count(byte[] query) {
    Range range = getRange(query);
    return range.second - range.first + 1;
  }

  /**
   * Get all locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Long[] search(byte[] query) {
    Range range = getRange(query);
    long sp = range.first, ep = range.second;
    if (ep - sp + 1 <= 0) {
      return new Long[0];
    }

    Long[] positions = new Long[(int) (ep - sp + 1)];
    for (long i = 0; i < ep - sp + 1; i++) {
      positions[(int) i] = lookupSA(sp + i) + fileOffset;
    }

    return positions;
  }

  /**
   * Performs regular expression search for an input expression using Succinct data-structures.
   *
   * @param query Regular expression pattern to be matched. (UTF-8 encoded)
   * @return All locations and lengths of matching patterns in original input.
   * @throws RegExParsingException
   */
  @Override public Map<Long, Integer> regexSearch(String query) throws RegExParsingException {
    RegExParser parser = new RegExParser(new String(query));
    RegEx regEx;

    regEx = parser.parse();

    RegExPlanner planner = new NaiveRegExPlanner(this, regEx);
    RegEx optRegEx = planner.plan();

    RegExExecutor regExExecutor = new RegExExecutor(this, optRegEx);
    regExExecutor.execute();

    Set<RegexMatch> chunkResults = regExExecutor.getFinalResults();
    Map<Long, Integer> results = new TreeMap<Long, Integer>();
    for (RegexMatch result : chunkResults) {
      results.put(result.getOffset() + fileOffset, result.getLength());
    }

    return results;
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  @Override public void readFromStream(DataInputStream is) {
    throw new UnsupportedOperationException("Cannot read SuccinctStream from another stream.");
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    byte[] buffer = new byte[1024];
    int len;
    while ((len = originalStream.read(buffer)) != -1) {
      os.write(buffer, 0, len);
    }
    originalStream.seek(0);
  }
}
