package edu.itu.bigdata;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TeraRecordReader extends RecordReader<Text, Text> {
	private FSDataInputStream in;
	private long offset;
	private long length;
	private static final int RECORD_LENGTH = TeraInputFormat.KEY_LENGTH + TeraInputFormat.VALUE_LENGTH;
	private byte[] buffer = new byte[RECORD_LENGTH];
	private Text key;
	private Text value;

	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) offset / length;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		Path p = ((FileSplit) split).getPath();
		FileSystem fs = p.getFileSystem(context.getConfiguration());
		in = fs.open(p);
		long start = ((FileSplit) split).getStart();
		// find the offset to start at a record boundary
		offset = (RECORD_LENGTH - (start % RECORD_LENGTH)) % RECORD_LENGTH;
		in.seek(start + offset);
		length = ((FileSplit) split).getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (offset >= length) {
			return false;
		}
		int read = 0;
		while (read < RECORD_LENGTH) {
			long newRead = in.read(buffer, read, RECORD_LENGTH - read);
			if (newRead == -1) {
				if (read == 0) {
					return false;
				} else {
					throw new EOFException("read past eof");
				}
			}
			read += newRead;
		}
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new Text();
		}
		key.set(buffer, 0, TeraInputFormat.KEY_LENGTH);
		value.set(buffer, TeraInputFormat.KEY_LENGTH, TeraInputFormat.VALUE_LENGTH);
		offset += RECORD_LENGTH;
		return true;
	}

}
