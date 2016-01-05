package edu.itu.bigdata.gen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class GenRecordReader extends RecordReader<LongWritable, NullWritable> {
	public static final String NUM_ROWS = "num-rows";;
	private long startRow;
	private long finishedRows;
	private long totalRows;
	private LongWritable key = null;

	@Override
	public void close() throws IOException {

	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return finishedRows / (float) totalRows;
	}

	@Override
	public void initialize(InputSplit spilter, TaskAttemptContext taskContext)
			throws IOException, InterruptedException {
		finishedRows = 0;
		startRow = ((GenSpilter) spilter).getStartRow();
		totalRows = ((GenSpilter) spilter).getRowCount();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		if (finishedRows < totalRows) {
			key.set(startRow + finishedRows);
			finishedRows += 1;
			return true;
		} else {
			return false;
		}
	}

	public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new GenRecordReader();
	}
}
