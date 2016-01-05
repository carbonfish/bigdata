package edu.itu.bigdata.sort;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.itu.bigdata.TeraInputFormat;

public class TotalOrderPartitioner extends Partitioner<Text, Text> implements Configurable {
	private TrieNode trie;
	private Text[] splitPoints;
	private Configuration conf;
	private static final Log LOG = LogFactory.getLog(TotalOrderPartitioner.class);


	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		try {
			FileSystem fs = FileSystem.getLocal(conf);
			this.conf = conf;
			Path partFile = new Path(TeraInputFormat.PARTITION_FILENAME);
			splitPoints = readPartitions(fs, partFile, conf);
			trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
		} catch (IOException ie) {
			throw new IllegalArgumentException("can't read partitions file", ie);
		}
	}

	private static TrieNode buildTrie(Text[] splits, int lower, int upper, Text prefix, int maxDepth) {
		int depth = prefix.getLength();
		if (depth >= maxDepth || lower == upper) {
			return new LeafTrieNode(depth, splits, lower, upper);
		}
		InnerTrieNode result = new InnerTrieNode(depth);
		Text trial = new Text(prefix);
		// append an extra byte on to the prefix
		trial.append(new byte[1], 0, 1);
		int currentBound = lower;
		for (int ch = 0; ch < 255; ++ch) {
			trial.getBytes()[depth] = (byte) (ch + 1);
			lower = currentBound;
			while (currentBound < upper) {
				if (splits[currentBound].compareTo(trial) >= 0) {
					break;
				}
				currentBound += 1;
			}
			trial.getBytes()[depth] = (byte) ch;
			result.getChild()[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
		}
		// pick up the rest
		trial.getBytes()[depth] = (byte) 255;
		result.getChild()[255] = buildTrie(splits, currentBound, upper, trial, maxDepth);
		return result;
	}

	private static Text[] readPartitions(FileSystem fs, Path partFile, Configuration conf) throws IOException {
		int reduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
		LOG.info("partFile : " + partFile.getName() );
		Text[] result = new Text[reduces - 1];
		DataInputStream reader = fs.open(partFile);
		for (int i = 0; i < reduces - 1; ++i) {
			result[i] = new Text();
			result[i].readFields(reader);
		}
		reader.close();
		return result;
	}

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return trie.findPartition(key);
	}
}
