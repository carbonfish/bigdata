package edu.itu.bigdata.sort;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Text;

public class InnerTrieNode extends TrieNode {
	private TrieNode[] child = new TrieNode[256];

	public InnerTrieNode(int depth) {
		super(depth);

	}

	@Override
	public int findPartition(Text key) {
		int level = getLevel();
		if (key.getLength() <= level) {
			return getChild()[0].findPartition(key);
		}
		return getChild()[key.getBytes()[level] & 0xff].findPartition(key);
	}

	@Override
	public void print(PrintStream strm) throws IOException {
		for (int ch = 0; ch < 256; ++ch) {
			for (int i = 0; i < 2 * getLevel(); ++i) {
				strm.print(' ');
			}
			strm.print(ch);
			strm.println(" ->");
			if (getChild()[ch] != null) {
				getChild()[ch].print(strm);
			}
		}
	}

	public TrieNode[] getChild() {
		return child;
	}

	public void setChild(int idx, TrieNode child) {
		this.child[idx] = child;
	}

}
