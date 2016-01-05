package edu.itu.bigdata.gen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 1: write to output stream. 2: close when job configuration does not require
 * sync. * @author pyu
 *
 */
public class TeraGenRecordWriter extends RecordWriter<Text, Text> {
	private boolean finalSync = false;
	private FSDataOutputStream out;

	public TeraGenRecordWriter(FSDataOutputStream fileOut, Configuration conf) {
		this.out = fileOut;
		finalSync = conf.getBoolean("isSync", false);

	}

	@Override
	public void close(TaskAttemptContext taskContext) throws IOException, InterruptedException {
		if (finalSync) {
			out.hflush();
		}
		out.close();
	}

	@Override
	public void write(Text key, Text value) throws IOException, InterruptedException {
		out.write(key.getBytes(), 0, key.getLength());
		out.write(value.getBytes(), 0, value.getLength());

	}

}
