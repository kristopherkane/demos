package com.demo.email;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;


public class EmailRecordReader extends RecordReader<LongWritable, Email>  {

    private static final Log LOG = LogFactory.getLog(EmailRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    boolean toggle = false;
    private LongWritable key = null;
    private Email email = null;
    private Text value = null;
    private Seekable filePosition;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes = null;

    public EmailRecordReader() {
    }

    public EmailRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        if (isCompressedInput()) {
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec)codec).createInputStream(
                                fileIn, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                in = new LineReader(cIn, job, recordDelimiterBytes);
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                filePosition = cIn;
            } else {
                in = new LineReader(codec.createInputStream(fileIn, decompressor), job,
                        recordDelimiterBytes);
                filePosition = fileIn;
            }
        } else {
            fileIn.seek(start);
            in = new LineReader(fileIn, job, recordDelimiterBytes);
            filePosition = fileIn;
        }
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }

    private boolean isCompressedInput() {
        return (codec != null);
    }

    private int maxBytesToConsume(long pos) {
        return isCompressedInput()
                ? Integer.MAX_VALUE
                : (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        email = new Email();
        toggle = false;
        value.clear();

        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        int counter = 0;
        while (getFilePosition() <= end) {
            counter +=1;
            LOG.info("Counter at: " + counter);
            newSize = in.readLine(value, maxLineLength,
                    Math.max(maxBytesToConsume(pos), maxLineLength));
            LOG.info("newSize: " + newSize);
            LOG.info("current value: " + value.toString());

            if (value.toString().startsWith("From:")) {
                if (toggle) {
                    LOG.info("Found the next record 'From' backing off and returning...");
                    pos = pos - newSize;
                    return true;
                }
                else {
                    LOG.info("############################################################################  - FROM -" + value.toString());
                    email.setFrom(value.toString());
                    toggle = true;
                }
            }

           else if (value.toString().startsWith("Date:")) {
                email.setDate(value.toString());

            }
           else if (value.toString().startsWith("Subject:")) {
                email.setSubject(value.toString());

            }
           else if (value.toString().startsWith("Message-ID:")) {
                email.setMessageID(value.toString());

            }
           else {
                LOG.info("Message ----------- " + value.toString());
                email.setMessage(value.toString());
            }
            if (newSize == 0) {
                break;
            }
            pos += newSize;

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " +
                    (pos - newSize));
        }
        if (newSize == 0) {
            // We've reached end of Split
            LOG.info("somehow didn't increment");
            key = null;
            value = null;
            return false;
        } else {
            LOG.info("at returning toggle");
            return toggle;
        }

    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Email getCurrentValue() {
        return email;
    }

    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            try {
                return Math.min(1.0f, (getFilePosition() - start)
                        / (float) (end - start));
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }
}
