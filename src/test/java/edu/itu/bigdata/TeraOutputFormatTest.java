package edu.itu.bigdata;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.JniBasedUnixGroupsMapping;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class TeraOutputFormatTest {
	@Mocked
	private JobContext job;

	@Mocked
	private Configuration conf;

	@Mocked
	private Path outDir;

	@Mocked
	private FileSystem fs;

	@Mocked
	private Credentials credentials;

	@Mocked
	private TaskAttemptContext taskAttempt;

	@Test(expected = InvalidJobConfException.class)
	public void checkOutputSpecsTestNullOutputDir() throws InvalidJobConfException, IOException {
		new Expectations() {
			{
				job.getConfiguration();
				result = conf;
			}
		};

		new Expectations() {
			{
				conf.get(FileOutputFormat.OUTDIR);
				result = null;
			}
		};

		TeraOutputFormat teraOutPutFormat = new TeraOutputFormat();
		teraOutPutFormat.checkOutputSpecs(job);
	}

	@Test()
	public void checkOutputSpecsTest() throws InvalidJobConfException, IOException {
		new Expectations() {
			{
				job.getConfiguration();
				result = conf;
			}
		};

		new Expectations() {

			{
				conf.get(FileOutputFormat.OUTDIR);
				result = "/";
			}
		};
		new Expectations() {

			{
				conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
				result = AuthenticationMethod.SIMPLE.toString();
			}
		};

		new Expectations() {

			{
				outDir.getFileSystem(conf);
				result = fs;
			}
		};

		new Expectations() {

			{
				job.getCredentials();
				result = credentials;
			}
		};
		
		new Expectations() {

			{
				conf.getClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, ShellBasedUnixGroupsMapping.class,
						GroupMappingServiceProvider.class);
				result = ShellBasedUnixGroupsMapping.class;
			}
		};

		new Expectations() {

			{
				long timeout = conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS,
						CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;
				result = 100;
			}
		};
		new Expectations() {

			{
				conf.getBoolean(anyString, anyBoolean);
				result = true;
			}
		};
		UserGroupInformation.setConfiguration(conf);
		TeraOutputFormat teraOutPutFormat = new TeraOutputFormat();
		teraOutPutFormat.checkOutputSpecs(job);
		
	}
	
	@Test
	public void getRecordWriterWriteTest() throws IOException, InterruptedException{
		setMockRecordWriterWrite();
		
		TeraOutputFormat teraOutPutFormat = new TeraOutputFormat();
		RecordWriter<Text,Text>  recordWriter = teraOutPutFormat.getRecordWriter(taskAttempt);
		Text key = new Text();
		Text value = new Text();
		recordWriter.write(key, value);
	}

	private void setMockRecordWriterWrite() {
		new Expectations() {
			{
				taskAttempt.getConfiguration();
				result = conf;
			}
		};
		new Expectations() {

			{
				conf.get(FileOutputFormat.OUTDIR);
				result = "/";
			}
		};
		new Expectations() {

			{
				conf.getInt(anyString,
	                    anyInt);
				result = 1;
			}
		};
		new Expectations() {

			{
				conf.get(anyString, anyString);
				result = "/";
			}
		};
	}
   
	@Test 
	public void getRecordWriterCloseTest() throws IOException, InterruptedException{
		setMockRecordWriterWrite();
		TeraOutputFormat teraOutPutFormat = new TeraOutputFormat();
		RecordWriter<Text,Text>  recordWriter = teraOutPutFormat.getRecordWriter(taskAttempt);
		Text key = new Text();
		Text value = new Text();
		recordWriter.close(taskAttempt);
	}
}
