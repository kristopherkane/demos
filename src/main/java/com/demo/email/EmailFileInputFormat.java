package com.demo.email;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * Created by kkane on 6/11/14.
 */
public class EmailFileInputFormat extends FileInputFormat<LongWritable, Email> {

    private static final Log LOG = LogFactory.getLog(EmailFileInputFormat.class);

    @Override
    public RecordReader<LongWritable, Email>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");

        LOG.info("Get a delimiter : [" + delimiter +  "]");

        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes();
        return new EmailRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }
}
