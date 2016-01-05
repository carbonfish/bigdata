package edu.itu.bigdata.gen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

public class GenSpilter extends InputSplit implements Writable {
	private long startRow;
	private long rowCount;

	public GenSpilter(long startRow, long rowCount) {
		this.startRow = startRow;
		this.rowCount = rowCount;
	}

	public GenSpilter() {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		startRow = WritableUtils.readVLong(in);
		rowCount = WritableUtils.readVLong(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVLong(out, startRow);
		WritableUtils.writeVLong(out, rowCount);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {};
	}

	public long getStartRow() {
		return this.startRow;
	}

	public long getRowCount() {
		return this.rowCount;
	}

}
