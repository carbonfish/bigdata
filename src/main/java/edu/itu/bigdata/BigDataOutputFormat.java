package edu.itu.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import edu.itu.bigdata.gen.Parameters;
import edu.itu.bigdata.gen.TeraGenRecordWriter;

/**
 * 1: check output file is existed. 2: get TeraGenRecordWriter.
 * 
 * @author pyu
 *
 */

public class BigDataOutputFormat extends FileOutputFormat<Text, Text> {
	private OutputCommitter committer = null;

	@Override
	public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
		Path outDir = getOutputPath(job);
		if (null == outDir) {
			throw new InvalidJobConfException("output directory is required");
		}
		final Configuration jobConf = job.getConfiguration();
		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { outDir }, jobConf);

		final FileSystem fs = outDir.getFileSystem(jobConf);

		if (fs.exists(outDir)) {
			// existing output dir is considered empty iff its only content is
			// the
			// partition file.
			//
			final FileStatus[] outDirKids = fs.listStatus(outDir);
			boolean empty = false;
			if (outDirKids != null && outDirKids.length == 1) {
				final FileStatus st = outDirKids[0];
				final String filename = st.getPath().getName();
				empty = !st.isDirectory() && Parameters.PARTITION_FILENAME.equals(filename);
			}
			/*
			 * if (TeraSort.getUseSimplePartitioner(job) || !empty) { throw new
			 * FileAlreadyExistsException("Output directory " + outDir +
			 * " already exists"); }
			 */
		}
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new FileOutputCommitter(output, context);
		}
		return committer;
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Path file = getDefaultWorkFile(job, "");
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file);
		return new TeraGenRecordWriter(fileOut, job.getConfiguration());
	}

}
