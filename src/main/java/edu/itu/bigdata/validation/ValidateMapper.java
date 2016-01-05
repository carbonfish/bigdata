package edu.itu.bigdata.validation;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.PureJavaCrc32;

import edu.itu.bigdata.gen.Unsigned16;

public class ValidateMapper extends Mapper<Text, Text, Text, Text> {
	private Text lastKey;
	private String filename;
	private Unsigned16 checksum = new Unsigned16();
	private Unsigned16 tmp = new Unsigned16();
	private Checksum crc32 = new PureJavaCrc32();

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (lastKey == null) {
			FileSplit fs = (FileSplit) context.getInputSplit();
			filename = getFilename(fs.getPath());
			context.write(new Text(filename + ":begin"), key);
			lastKey = new Text();
		} else {
			if (key.compareTo(lastKey) < 0) {
				context.write(Validation.ERROR, new Text("misorder in " + filename + " between "
						+ Validation.textifyBytes(lastKey) + " and " + Validation.textifyBytes(key)));
			}
		}
		crc32.reset();
		crc32.update(key.getBytes(), 0, key.getLength());
		crc32.update(value.getBytes(), 0, value.getLength());
		tmp.set(crc32.getValue());
		checksum.add(tmp);
		lastKey.set(key);
	}

	private String getFilename(Path path) {
		return path.getName();
	}

}
