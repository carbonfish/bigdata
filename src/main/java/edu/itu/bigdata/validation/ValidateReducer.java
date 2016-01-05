package edu.itu.bigdata.validation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.itu.bigdata.gen.Unsigned16;

public class ValidateReducer extends Reducer<Text, Text, Text, Text> {
	private boolean firstKey = true;
	private Text lastKey = new Text();
	private Text lastValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (Validation.ERROR.equals(key)) {
			for (Text val : values) {
				context.write(key, val);
			}
		} else if (Validation.CHECKSUM.equals(key)) {
			Unsigned16 tmp = new Unsigned16();
			Unsigned16 sum = new Unsigned16();
			for (Text val : values) {
				tmp.set(val.toString());
				sum.add(tmp);
			}
			context.write(Validation.CHECKSUM, new Text(sum.toString()));
		} else {
			Text value = values.iterator().next();
			if (firstKey) {
				firstKey = false;
			} else {
				if (value.compareTo(lastValue) < 0) {
					context.write(Validation.ERROR,
							new Text("bad key partitioning:\n  file " + lastKey + " key "
									+ Validation.textifyBytes(lastValue) + "\n  file " + key + " key "
									+ Validation.textifyBytes(value)));
				}
			}
			lastKey.set(key);
			lastValue.set(value);
		}
	}
}
