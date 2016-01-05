package edu.itu.bigdata.gen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RangeInputFormat  extends InputFormat<LongWritable, NullWritable>{
	

	@Override
	public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return new GenRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
		  long totalRows = getNumberOfRows(job);
	      int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
	      List<InputSplit> splits = new ArrayList<InputSplit>();
	      long currentRow = 0;
	      for(int split = 0; split < numSplits; ++split) {
	        long goal = 
	          (long) Math.ceil(totalRows * (double)(split + 1) / numSplits);
	        splits.add(new GenSpilter(currentRow, goal - currentRow));
	        currentRow = goal;
	      }
	      return splits;
	}

	private long getNumberOfRows(JobContext job) {
		return job.getConfiguration().getLong(GenRecordReader.NUM_ROWS, 0);
	}

}
