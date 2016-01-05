package edu.itu.bigdata.validation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.itu.bigdata.TeraInputFormat;

public class Validation extends Configured implements Tool {
	public static final Text ERROR = new Text("error");
	public static final Text CHECKSUM = new Text("checksum");

	public static String textifyBytes(Text t) {
		BytesWritable b = new BytesWritable();
		b.set(t.getBytes(), 0, t.getLength());
		return b.toString();
	}

	private static void usage() throws IOException {
		System.err.println("validate <out-dir> <report-dir>");
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		if (args.length != 2) {
			usage();
			String[] temp = { "Test_out","Test_report" };
			args = temp;
		}
		TeraInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJobName("validate");
		job.setJarByClass(Validation.class);
		job.setMapperClass(ValidateMapper.class);
		job.setReducerClass(ValidateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// force a single reducer
		job.setNumReduceTasks(1);
		// force a single split
		FileInputFormat.setMinInputSplitSize(job, Long.MAX_VALUE);
		job.setInputFormatClass(TeraInputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Validation(), args);
		System.exit(res);
	}
}
