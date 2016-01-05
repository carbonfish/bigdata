package edu.itu.bigdata.sort;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Text;

public class LeafTrieNode extends TrieNode {
	private int lower;
	private int upper;
	private Text[] splitPoints;

	LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
		super(level);
		this.splitPoints = splitPoints;
		this.lower = lower;
		this.upper = upper;
	}

	@Override
	public int findPartition(Text key) {
		for (int i = lower; i < upper; ++i) {
			if (splitPoints[i].compareTo(key) > 0) {
				return i;
			}
		}
		return upper;
	}

	@Override
	public void print(PrintStream strm) throws IOException {
		for (int i = 0; i < 2 * getLevel(); ++i) {
			strm.print(' ');
		}
		strm.print(lower);
		strm.print(", ");
		strm.println(upper);
	}
}
