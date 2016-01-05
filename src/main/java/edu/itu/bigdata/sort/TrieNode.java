package edu.itu.bigdata.sort;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Text;

public abstract class TrieNode {
	private int level;

	TrieNode(int level) {
		this.level = level;
	}

	public abstract  int findPartition(Text key);

	public abstract void print(PrintStream strm) throws IOException;

	protected int getLevel() {
		return level;
	}

}
