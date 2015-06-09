package edu.berkeley.cs.succinct.util.streams;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public class TestUtils {

    public static FSDataInputStream getStream(LongBuffer buf) throws IOException {
        File tmpDir = Files.createTempDir();
        Path filePath = new Path(tmpDir.getAbsolutePath() + "/testOut");
        FileSystem fs = FileSystem.get(filePath.toUri(), new Configuration());
        FSDataOutputStream fOut = fs.create(filePath);
        buf.rewind();
        while(buf.hasRemaining()) {
            fOut.writeLong(buf.get());
        }
        fOut.close();
        buf.rewind();
        return fs.open(filePath);
    }

    public static FSDataInputStream getStream(ShortBuffer buf) throws IOException {
        File tmpDir = Files.createTempDir();
        Path filePath = new Path(tmpDir.getAbsolutePath() + "/testOut");
        FileSystem fs = FileSystem.get(filePath.toUri(), new Configuration());
        FSDataOutputStream fOut = fs.create(filePath);
        buf.rewind();
        while(buf.hasRemaining()) {
            fOut.writeShort(buf.get());
        }
        fOut.close();
        buf.rewind();
        return fs.open(filePath);
    }

    public static FSDataInputStream getStream(IntBuffer buf) throws IOException {
        File tmpDir = Files.createTempDir();
        Path filePath = new Path(tmpDir.getAbsolutePath() + "/testOut");
        FileSystem fs = FileSystem.get(filePath.toUri(), new Configuration());
        FSDataOutputStream fOut = fs.create(filePath);
        buf.rewind();
        while(buf.hasRemaining()) {
            fOut.writeInt(buf.get());
        }
        fOut.close();
        buf.rewind();
        return fs.open(filePath);
    }

    public static FSDataInputStream getStream(ByteBuffer buf) throws IOException {
        File tmpDir = Files.createTempDir();
        Path filePath = new Path(tmpDir.getAbsolutePath() + "/testOut");
        FileSystem fs = FileSystem.get(filePath.toUri(), new Configuration());
        FSDataOutputStream fOut = fs.create(filePath);
        buf.rewind();
        while(buf.hasRemaining()) {
            fOut.writeByte(buf.get());
        }
        fOut.close();
        buf.rewind();
        return fs.open(filePath);
    }

}
