/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.serializer2;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public final class SerializationOutputStream extends OutputStream {

	private final OutputStream outputStream;
	private final byte[] buf;
	private int pos;

	public SerializationOutputStream(OutputStream outputStream, int bufferSize) {
		this.outputStream = outputStream;
		this.buf = new byte[bufferSize];
		this.pos = 0;
	}

	@Override
	public void flush() throws IOException {
		if (pos != 0) {
			outputStream.write(buf, 0, pos);
			pos = 0;
		}
	}

	@Override
	public void close() throws IOException {
		outputStream.close();
	}

	private int remaining() {
		return buf.length - pos;
	}

	private void ensureSize(int size) throws IOException {
		if (remaining() < size) {
			flush();
		}
	}

	@Override
	public void write(int b) throws IOException {
		writeByte((byte) b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (len < buf.length) {
			ensureSize(len);
			System.arraycopy(b, off, buf, pos, len);
			pos += len;
		} else {
			flush();
			outputStream.write(b, off, len);
		}
	}

	public void writeBoolean(boolean v) throws IOException {
		writeByte(v ? (byte) 1 : 0);
	}

	public void writeByte(byte v) throws IOException {
		ensureSize(1);
		buf[pos++] = v;
	}

	public void writeChar(char v) throws IOException {
		ensureSize(2);
		writeByte((byte) (v >>> 8));
		writeByte((byte) (v));
	}

	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	public void writeFloat(float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	public void writeInt(int val) throws IOException {
		ensureSize(4);
		buf[pos] = (byte) (val >>> 24);
		buf[pos + 1] = (byte) (val >>> 16);
		buf[pos + 2] = (byte) (val >>> 8);
		buf[pos + 3] = (byte) (val);
		pos += 4;
	}

	public void writeLong(long val) throws IOException {
		ensureSize(8);
		int high = (int) (val >>> 32);
		int low = (int) val;
		buf[pos] = (byte) (high >>> 24);
		buf[pos + 1] = (byte) (high >>> 16);
		buf[pos + 2] = (byte) (high >>> 8);
		buf[pos + 3] = (byte) high;
		buf[pos + 4] = (byte) (low >>> 24);
		buf[pos + 5] = (byte) (low >>> 16);
		buf[pos + 6] = (byte) (low >>> 8);
		buf[pos + 7] = (byte) low;
		pos += 8;
	}

	public void writeShort(short v) throws IOException {
		ensureSize(2);
		buf[pos] = (byte) (v >>> 8);
		buf[pos + 1] = (byte) (v);
		pos += 2;
	}

	public void writeVarInt(int value) throws IOException {
		ensureSize(5);
		if ((value & ~0x7F) == 0) {
			buf[pos] = (byte) value;
			pos += 1;
			return;
		}
		buf[pos] = (byte) ((value & 0x7F) | 0x80);
		value >>>= 7;
		if ((value & ~0x7F) == 0) {
			buf[pos + 1] = (byte) value;
			pos += 2;
			return;
		}
		buf[pos + 1] = (byte) ((value & 0x7F) | 0x80);
		value >>>= 7;
		if ((value & ~0x7F) == 0) {
			buf[pos + 2] = (byte) value;
			pos += 3;
			return;
		}
		buf[pos + 2] = (byte) ((value & 0x7F) | 0x80);
		value >>>= 7;
		if ((value & ~0x7F) == 0) {
			buf[pos + 3] = (byte) value;
			pos += 4;
			return;
		}
		buf[pos + 3] = (byte) ((value & 0x7F) | 0x80);
		value >>>= 7;
		buf[pos + 4] = (byte) value;
		pos += 5;
	}

	public void writeVarLong(long value) throws IOException {
		ensureSize(9);
		if ((value & ~0x7F) == 0) {
			writeByte((byte) value);
		} else {
			writeByte((byte) ((value & 0x7F) | 0x80));
			value >>>= 7;
			if ((value & ~0x7F) == 0) {
				writeByte((byte) value);
			} else {
				writeByte((byte) ((value & 0x7F) | 0x80));
				value >>>= 7;
				if ((value & ~0x7F) == 0) {
					writeByte((byte) value);
				} else {
					writeByte((byte) ((value & 0x7F) | 0x80));
					value >>>= 7;
					if ((value & ~0x7F) == 0) {
						writeByte((byte) value);
					} else {
						writeByte((byte) ((value & 0x7F) | 0x80));
						value >>>= 7;
						if ((value & ~0x7F) == 0) {
							writeByte((byte) value);
						} else {
							writeByte((byte) ((value & 0x7F) | 0x80));
							value >>>= 7;
							if ((value & ~0x7F) == 0) {
								writeByte((byte) value);
							} else {
								writeByte((byte) ((value & 0x7F) | 0x80));
								value >>>= 7;

								for (; ; ) {
									if ((value & ~0x7FL) == 0) {
										writeByte((byte) value);
										return;
									} else {
										writeByte((byte) (((int) value & 0x7F) | 0x80));
										value >>>= 7;
									}
								}
							}
						}
					}
				}
			}
		}
	}

	public void writeUTF8(String value) throws IOException {
		int length = value.length();
		writeVarInt(length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = value.charAt(i);
			if (c <= 0x007F) {
				buf[pos++] = (byte) c;
			} else {
				writeUtfChar(c);
			}
		}
	}

	public void writeNullableUTF8(String value) throws IOException {
		if (value == null) {
			writeByte((byte) 0);
			return;
		}
		int length = value.length();
		writeVarInt(length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = value.charAt(i);
			if (c <= 0x007F) {
				buf[pos++] = (byte) c;
			} else {
				writeUtfChar(c);
			}
		}
	}

	private void writeUtfChar(int c) {
		if (c <= 0x07FF) {
			buf[pos] = (byte) (0xC0 | c >> 6 & 0x1F);
			buf[pos + 1] = (byte) (0x80 | c & 0x3F);
			pos += 2;
		} else {
			buf[pos] = (byte) (0xE0 | c >> 12 & 0x0F);
			buf[pos + 1] = (byte) (0x80 | c >> 6 & 0x3F);
			buf[pos + 2] = (byte) (0x80 | c & 0x3F);
			pos += 3;
		}
	}

	public final void writeUTF16(String s) throws IOException {
		int length = s.length();
		writeVarInt(length);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			writeByte((byte) (v >>> 8));
			writeByte((byte) (v));
		}
	}

	public final void writeNullableUTF16(String s) throws IOException {
		if (s == null) {
			writeByte((byte) 0);
			return;
		}
		int length = s.length();
		writeVarInt(length + 1);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			writeByte((byte) (v >>> 8));
			writeByte((byte) (v));
		}
	}

	@Override
	public String toString() {
		return "ArrayOutputBuffer [pos: " + pos + ", buf" + (buf.length < 100 ? ": " + Arrays.toString(buf) : " size " + buf.length) + "]";
	}

}
