package edu.itu.bigdata.gen;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.PureJavaCrc32;

public class GenMapper extends Mapper<LongWritable, NullWritable, Text, Text> {

	public static final int KEY_LENGTH = 10;
	public static final int VALUE_LENGTH = 90;
	private Text key = new Text();
	private Text value = new Text();
	private Checksum crc32 = new PureJavaCrc32();
	private Unsigned16 rand;
	private Unsigned16 rowId;
	private Counter checksumCounter;
	private Unsigned16 total = new Unsigned16();

	private Unsigned16 checksum = new Unsigned16();

	private byte[] buffer = new byte[KEY_LENGTH + VALUE_LENGTH];
	private static final Unsigned16 ONE = new Unsigned16(1);

	public static enum Counters {
		CHECKSUM
	}

	@Override
	protected void map(LongWritable row, NullWritable ignorable,
			Mapper<LongWritable, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		if (rand == null) {
			rowId = new Unsigned16(row.get());
			rand = Random16.skipAhead(rowId);
			checksumCounter = context.getCounter(Counters.CHECKSUM);
		}
		Random16.nextRand(rand);
		generateRecord(buffer, rand, rowId);
		key.set(buffer, 0, KEY_LENGTH);
		value.set(buffer, KEY_LENGTH, VALUE_LENGTH);
		context.write(key, value);
		crc32.reset();
		crc32.update(buffer, 0, KEY_LENGTH + VALUE_LENGTH);
		checksum.set(crc32.getValue());
		total.add(checksum);
		rowId.add(ONE);
	}

	private void generateRecord(byte[] buffer, Unsigned16 rand2, Unsigned16 recordNumber) {
		for (int i = 0; i < 10; ++i) {
			buffer[i] = rand.getByte(i);
		}
		buffer[10] = 0x00;
		buffer[11] = 0x11;
		for (int i = 0; i < 32; i++) {
			buffer[12 + i] = (byte) recordNumber.getHexDigit(i);
		}
		buffer[44] = (byte) 0x88;
		buffer[45] = (byte) 0x99;
		buffer[46] = (byte) 0xAA;
		buffer[47] = (byte) 0xBB;
		for (int i = 0; i < 12; ++i) {
			buffer[48 + i * 4] = buffer[49
					+ i * 4] = buffer[50 + i * 4] = buffer[51 + i * 4] = (byte) rand.getHexDigit(20 + i);
		}
		buffer[96] = (byte) 0xCC;
		buffer[97] = (byte) 0xDD;
		buffer[98] = (byte) 0xEE;
		buffer[99] = (byte) 0xFF;
	}

	@Override
	public void cleanup(Context context) {
		if (checksumCounter != null) {
			checksumCounter.increment(total.getLow8());
		}
	}
}
