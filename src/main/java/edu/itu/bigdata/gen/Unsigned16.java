package edu.itu.bigdata.gen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Unsigned16 implements Writable {
	private long hi8;
	private long lo8;

	public Unsigned16() {
		hi8 = 0;
		lo8 = 0;
	}

	public Unsigned16(long l) {
		hi8 = 0;
		lo8 = l;
	}

	public Unsigned16(Unsigned16 other) {
		hi8 = other.hi8;
		lo8 = other.lo8;
	}

	public Unsigned16(String s) throws NumberFormatException {
		this.set(s);
	}

	public void set(String s) throws NumberFormatException {
		hi8 = 0;
		lo8 = 0;
		final long lastDigit = 0xfl << 60;
		for (int i = 0; i < s.length(); ++i) {
			int digit = getHexDigit(s.charAt(i));
			if ((lastDigit & hi8) != 0) {
				throw new NumberFormatException(s + " overflowed 16 bytes");
			}
			hi8 <<= 4;
			hi8 |= (lo8 & lastDigit) >>> 60;
			lo8 <<= 4;
			lo8 |= digit;
		}
	}

	private int getHexDigit(char ch) throws NumberFormatException {
		if (ch >= '0' && ch <= '9') {
			return ch - '0';
		}
		if (ch >= 'a' && ch <= 'f') {
			return ch - 'a' + 10;
		}
		if (ch >= 'A' && ch <= 'F') {
			return ch - 'A' + 10;
		}
		throw new NumberFormatException(ch + " is not a valid hex digit");
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Unsigned16) {
			Unsigned16 other = (Unsigned16) o;
			return other.hi8 == hi8 && other.lo8 == lo8;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (int) lo8;
	}

	public String toString() {
		if (hi8 == 0) {
			return Long.toHexString(lo8);
		} else {
			StringBuilder result = new StringBuilder();
			result.append(Long.toHexString(hi8));
			String loString = Long.toHexString(lo8);
			for (int i = loString.length(); i < 16; ++i) {
				result.append('0');
			}
			result.append(loString);
			return result.toString();
		}
	}

	public byte getByte(int b) {
		if (b >= 0 && b < 16) {
			if (b < 8) {
				return (byte) (hi8 >> (56 - 8 * b));
			} else {
				return (byte) (lo8 >> (120 - 8 * b));
			}
		}
		return 0;
	}

	public char getHexDigit(int p) {
		byte digit = getByte(p / 2);
		if (p % 2 == 0) {
			digit >>>= 4;
		}
		digit &= 0xf;
		if (digit < 10) {
			return (char) ('0' + digit);
		} else {
			return (char) ('A' + digit - 10);
		}
	}

	void multiply(Unsigned16 b) {
		// divide the left into 4 32 bit chunks
		long[] left = new long[4];
		left[0] = lo8 & 0xffffffffl;
		left[1] = lo8 >>> 32;
		left[2] = hi8 & 0xffffffffl;
		left[3] = hi8 >>> 32;
		// divide the right into 5 31 bit chunks
		long[] right = new long[5];
		right[0] = b.lo8 & 0x7fffffffl;
		right[1] = (b.lo8 >>> 31) & 0x7fffffffl;
		right[2] = (b.lo8 >>> 62) + ((b.hi8 & 0x1fffffffl) << 2);
		right[3] = (b.hi8 >>> 29) & 0x7fffffffl;
		right[4] = (b.hi8 >>> 60);
		// clear the cur value
		set(0);
		Unsigned16 tmp = new Unsigned16();
		for (int l = 0; l < 4; ++l) {
			for (int r = 0; r < 5; ++r) {
				long prod = left[l] * right[r];
				if (prod != 0) {
					int off = l * 32 + r * 31;
					tmp.set(prod);
					tmp.shiftLeft(off);
					add(tmp);
				}
			}
		}
	}

	public void add(Unsigned16 b) {
		long sumHi;
		long sumLo;
		long reshibit, hibit0, hibit1;

		sumHi = hi8 + b.hi8;

		hibit0 = (lo8 & 0x8000000000000000L);
		hibit1 = (b.lo8 & 0x8000000000000000L);
		sumLo = lo8 + b.lo8;
		reshibit = (sumLo & 0x8000000000000000L);
		if ((hibit0 & hibit1) != 0 | ((hibit0 ^ hibit1) != 0 && reshibit == 0))
			sumHi++; /* add carry bit */
		hi8 = sumHi;
		lo8 = sumLo;
	}

	public void shiftLeft(int bits) {
		if (bits != 0) {
			if (bits < 64) {
				hi8 <<= bits;
				hi8 |= (lo8 >>> (64 - bits));
				lo8 <<= bits;
			} else if (bits < 128) {
				hi8 = lo8 << (bits - 64);
				lo8 = 0;
			} else {
				hi8 = 0;
				lo8 = 0;
			}
		}
	}

	public void set(long l) {
		lo8 = l;
		hi8 = 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		hi8 = in.readLong();
		lo8 = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(hi8);
		out.writeLong(lo8);
	}

	public long getLow8() {
		return lo8;
	}

	public long getHigh8() {
		return hi8;
	}

}
