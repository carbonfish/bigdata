package edu.itu.bigdata.sort;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.itu.bigdata.BigDataOutputFormat;
import edu.itu.bigdata.TeraInputFormat;
import edu.itu.bigdata.gen.Parameters;

public class Sort extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(Sort.class);
	private static final String SIMPLE_PARTITIONER = "simplepartitioner";
	private static final String OUTPUT_REPLICATION = "output.replication";

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("Starting Sort....");
		Job job = Job.getInstance(getConf());
		if (args.length != 2) {
			LOG.info("Test");
			String[] temp = { "Test","Test_out" };
			args = temp;
		}
		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		boolean useSimplePartitioner = getUseSimplePartitioner(job.getConfiguration());
		TeraInputFormat.setInputPaths(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("TeraSort");
		job.setJarByClass(Sort.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TeraInputFormat.class);
		job.setOutputFormatClass(BigDataOutputFormat.class);
		job.setPartitionerClass(SimplePartitioner.class);

		if (useSimplePartitioner) {
			job.setPartitionerClass(SimplePartitioner.class);
		} else {
			long start = System.currentTimeMillis();
			Path partitionFile = new Path(outputDir, TeraInputFormat.PARTITION_FILENAME);
			URI partitionUri = new URI(partitionFile.toString() + "#" + TeraInputFormat.PARTITION_FILENAME);
			try {
				TeraInputFormat.writePartitionFile(job, partitionFile);
			} catch (Throwable e) {
				LOG.error(e.getMessage());
				return -1;
			}
			job.addCacheFile(partitionUri);
			long end = System.currentTimeMillis();
			System.out.println("Spent " + (end - start) + "ms computing partitions.");
			job.setPartitionerClass(TotalOrderPartitioner.class);
		}

		job.getConfiguration().setInt("dfs.replication", getOutputReplication(job.getConfiguration()));
		setFinalSync(job.getConfiguration(), true);
		int ret = job.waitForCompletion(true) ? 0 : 1;
		LOG.info("done");
		return ret;
	}

	private void setFinalSync(Configuration configuration, boolean newValue) {
		configuration.setBoolean(Parameters.FINAL_SYNC_ATTRIBUTE, newValue);
	}

	private int getOutputReplication(Configuration configuration) {
		return configuration.getInt(OUTPUT_REPLICATION, 1);
	}

	private boolean getUseSimplePartitioner(Configuration config) {
		return config.getBoolean(SIMPLE_PARTITIONER, false);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Sort(), args);
		System.exit(res);
	}

}
