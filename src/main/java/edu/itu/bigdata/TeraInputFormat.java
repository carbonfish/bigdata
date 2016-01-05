package edu.itu.bigdata;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.StringUtils;

public class TeraInputFormat extends FileInputFormat<Text, Text> {
	public static final String PARTITION_FILENAME = "_partition.lst";
	public static final String NUM_PARTITIONS = "num.partitions";
	public static final String SAMPLE_SIZE = "partitions.sample";
	public static final int KEY_LENGTH = 10;
	public static final int VALUE_LENGTH = 90;
	public static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;
	private static MRJobConfig lastContext = null;
	private static List<InputSplit> lastResult = null;

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TeraRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		if (job == lastContext) {
			return lastResult;
		}
		long t1, t2, t3;
		t1 = System.currentTimeMillis();
		lastContext = job;
		lastResult = super.getSplits(job);
		t2 = System.currentTimeMillis();
		System.out.println("Spent " + (t2 - t1) + "ms computing base-splits.");
		/*
		 * if (job.getConfiguration().getBoolean(TeraScheduler.USE, true)) {
		 * TeraScheduler scheduler = new TeraScheduler(lastResult.toArray(new
		 * FileSplit[0]), job.getConfiguration()); lastResult =
		 * scheduler.getNewFileSplits(); t3 = System.currentTimeMillis();
		 * System.out.println("Spent " + (t3 - t2) +
		 * "ms computing TeraScheduler splits."); }
		 */
		return lastResult;
	}

	public static void writePartitionFile(final Job job, Path partitionFile) throws Throwable {
		long t1 = System.currentTimeMillis();
		Configuration conf = job.getConfiguration();
		final TeraInputFormat inFormat = new TeraInputFormat();
		final TextSampler sampler = new TextSampler();
		int partitions = job.getNumReduceTasks();
		long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
		final List<InputSplit> splits = inFormat.getSplits(job);
		long t2 = System.currentTimeMillis();
		System.out.println("Computing input splits took " + (t2 - t1) + "ms");
		int samples = Math.min(conf.getInt(NUM_PARTITIONS, 10), splits.size());
		System.out.println("Sampling " + samples + " splits of " + splits.size());
		final long recordsPerSample = sampleSize / samples;
		final int sampleStep = splits.size() / samples;
		Thread[] samplerReader = new Thread[samples];
		SamplerThreadGroup threadGroup = new SamplerThreadGroup("Sampler Reader Thread Group");
		// take N samples from different parts of the input
		for (int i = 0; i < samples; ++i) {
			final int idx = i;
			samplerReader[i] = new Thread(threadGroup, "Sampler Reader " + idx) {
				{
					setDaemon(true);
				}

				public void run() {
					long records = 0;
					try {
						TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(),
								new TaskAttemptID());
						RecordReader<Text, Text> reader = inFormat.createRecordReader(splits.get(sampleStep * idx),
								context);
						reader.initialize(splits.get(sampleStep * idx), context);
						while (reader.nextKeyValue()) {
							sampler.addKey(new Text(reader.getCurrentKey()));
							records += 1;
							if (recordsPerSample <= records) {
								break;
							}
						}
					} catch (IOException ie) {
						System.err
								.println("Got an exception while reading splits " + StringUtils.stringifyException(ie));
						throw new RuntimeException(ie);
					} catch (InterruptedException e) {

					}
				}
			};
			samplerReader[i].start();
		}
		FileSystem outFs = partitionFile.getFileSystem(conf);
		DataOutputStream writer = outFs.create(partitionFile, true, 64 * 1024, (short) 10,
				outFs.getDefaultBlockSize(partitionFile));
		for (int i = 0; i < samples; i++) {
			try {
				samplerReader[i].join();
				if (threadGroup.getThrowable() != null) {
					throw threadGroup.getThrowable();
				}
			} catch (InterruptedException e) {
			}
		}
		for (Text split : sampler.createPartitions(partitions)) {
			split.write(writer);
		}
		writer.close();
		long t3 = System.currentTimeMillis();
		System.out.println("Computing parititions took " + (t3 - t2) + "ms");
	}

}
