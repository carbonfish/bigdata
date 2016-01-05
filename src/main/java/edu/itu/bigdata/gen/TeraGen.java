package edu.itu.bigdata.gen;

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

public class TeraGen extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(TeraGen.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TeraGen(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			usage();
			LOG.info("Test");
			String[] temp = { "2", "Test" };
			args = temp;
		}
		conf.setLong(GenRecordReader.NUM_ROWS, Long.parseLong(args[0]));
		Job job = Job.getInstance(conf, "Tera Gen");
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJarByClass(TeraGen.class);
		job.setMapperClass(GenMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(RangeInputFormat.class);
		job.setOutputFormatClass(BigDataOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void usage() {
		System.err.println("TeraGenDriver <num rows> <output dir>");

	}

}
