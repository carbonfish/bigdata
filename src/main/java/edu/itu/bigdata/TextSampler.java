package edu.itu.bigdata;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class TextSampler implements IndexedSortable {
	private ArrayList<Text> records = new ArrayList<Text>();

	@Override
	public int compare(int i, int j) {
		Text left = records.get(i);
		Text right = records.get(j);
		return left.compareTo(right);
	}

	@Override
	public void swap(int i, int j) {
		Text left = records.get(i);
		Text right = records.get(j);
		records.set(j, left);
		records.set(i, right);
	}

	Text[] createPartitions(int numPartitions) {
		int numRecords = records.size();
		System.out.println("Making " + numPartitions + " from " + numRecords + " sampled records");
		if (numPartitions > numRecords) {
			throw new IllegalArgumentException(
					"Requested more partitions than input keys (" + numPartitions + " > " + numRecords + ")");
		}
		new QuickSort().sort(this, 0, records.size());
		float stepSize = numRecords / (float) numPartitions;
		Text[] result = new Text[numPartitions - 1];
		for (int i = 1; i < numPartitions; ++i) {
			result[i - 1] = records.get(Math.round(stepSize * i));
		}
		return result;
	}

	public void addKey(Text key) {
		synchronized (this) {
			records.add(new Text(key));
		}
	}
}
