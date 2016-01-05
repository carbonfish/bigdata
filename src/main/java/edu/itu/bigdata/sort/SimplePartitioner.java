package edu.itu.bigdata.sort;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;

public class SimplePartitioner extends Partitioner<Text, Text> implements Configurable {
	private int prefixesPerReduce;
	private static final int PREFIX_LENGTH = 3;
	private Configuration conf = null;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		prefixesPerReduce = (int) Math
				.ceil((1 << (8 * PREFIX_LENGTH)) / (float) conf.getInt(MRJobConfig.NUM_REDUCES, 1));
	}

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		byte[] bytes = key.getBytes();
		int len = Math.min(PREFIX_LENGTH, key.getLength());
		int prefix = 0;
		for (int i = 0; i < len; ++i) {
			prefix = (prefix << 8) | (0xff & bytes[i]);
		}
		return prefix / prefixesPerReduce;
	} 
}
