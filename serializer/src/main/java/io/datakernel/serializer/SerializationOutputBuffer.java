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

package io.datakernel.serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class SerializationOutputBuffer {
	protected byte[] buf;
	protected int pos;

	public SerializationOutputBuffer() {
	}

	public SerializationOutputBuffer(byte[] array) {
		this.buf = array;
		this.pos = 0;
	}

	public SerializationOutputBuffer(byte[] array, int position) {
		this.buf = array;
		this.pos = position;
	}

	public void set(byte[] buf, int pos) {
		this.buf = buf;
		this.pos = pos;
	}

	public byte[] array() {
		return buf;
	}

	public int position() {
		return pos;
	}

	public void position(int pos) {
		this.pos = pos;
	}

	public ByteBuffer getByteBuffer() {
		return ByteBuffer.wrap(buf, 0, pos);
	}

	public int remaining() {
		return buf.length - pos;
	}

	protected void ensureSize(int size) {
	}

	public void write(byte[] b) {
		write(b, 0, b.length);
	}

	public void write(byte[] b, int off, int len) {
		ensureSize(len);
		System.arraycopy(b, off, buf, pos, len);
		pos += len;
	}

	public void writeBoolean(boolean v) {
		writeByte(v ? (byte) 1 : 0);
	}

	public void writeByte(byte v) {
		ensureSize(1);
		buf[pos++] = v;
	}

	public void writeChar(char v) {
		ensureSize(2);
		writeByte((byte) (v >>> 8));
		writeByte((byte) (v));
	}

	public void writeDouble(double v) {
		writeLong(Double.doubleToLongBits(v));
	}

	public void writeFloat(float v) {
		writeInt(Float.floatToIntBits(v));
	}

	public void writeInt(int v) {
		ensureSize(4);
		buf[pos] = (byte) (v >>> 24);
		buf[pos + 1] = (byte) (v >>> 16);
		buf[pos + 2] = (byte) (v >>> 8);
		buf[pos + 3] = (byte) (v);
		pos += 4;
	}

	public void writeLong(long v) {
		ensureSize(8);
		int high = (int) (v >>> 32);
		int low = (int) v;
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

	public void writeShort(short v) {
		ensureSize(2);
		buf[pos] = (byte) (v >>> 8);
		buf[pos + 1] = (byte) (v);
		pos += 2;
	}

	public void writeVarInt(int v) {
		ensureSize(5);
		if ((v & ~0x7F) == 0) {
			buf[pos] = (byte) v;
			pos += 1;
			return;
		}
		buf[pos] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[pos + 1] = (byte) v;
			pos += 2;
			return;
		}
		buf[pos + 1] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[pos + 2] = (byte) v;
			pos += 3;
			return;
		}
		buf[pos + 2] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[pos + 3] = (byte) v;
			pos += 4;
			return;
		}
		buf[pos + 3] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		buf[pos + 4] = (byte) v;
		pos += 5;
	}

	public void writeVarLong(long v) {
		ensureSize(9);
		if ((v & ~0x7F) == 0) {
			writeByte((byte) v);
		} else {
			writeByte((byte) ((v & 0x7F) | 0x80));
			v >>>= 7;
			if ((v & ~0x7F) == 0) {
				writeByte((byte) v);
			} else {
				writeByte((byte) ((v & 0x7F) | 0x80));
				v >>>= 7;
				if ((v & ~0x7F) == 0) {
					writeByte((byte) v);
				} else {
					writeByte((byte) ((v & 0x7F) | 0x80));
					v >>>= 7;
					if ((v & ~0x7F) == 0) {
						writeByte((byte) v);
					} else {
						writeByte((byte) ((v & 0x7F) | 0x80));
						v >>>= 7;
						if ((v & ~0x7F) == 0) {
							writeByte((byte) v);
						} else {
							writeByte((byte) ((v & 0x7F) | 0x80));
							v >>>= 7;
							if ((v & ~0x7F) == 0) {
								writeByte((byte) v);
							} else {
								writeByte((byte) ((v & 0x7F) | 0x80));
								v >>>= 7;

								for (; ; ) {
									if ((v & ~0x7FL) == 0) {
										writeByte((byte) v);
										return;
									} else {
										writeByte((byte) (((int) v & 0x7F) | 0x80));
										v >>>= 7;
									}
								}
							}
						}
					}
				}
			}
		}
	}

	public void writeIso88591(String s) {
		int length = s.length();
		writeVarInt(length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			buf[pos++] = (byte) c;
		}
	}

	public void writeNullableIso88591(String s) {
		if (s == null) {
			writeByte((byte) 0);
			return;
		}
		int length = s.length();
		writeVarInt(length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			buf[pos++] = (byte) c;
		}
	}

	public void writeJavaUTF8(String s) {
		try {
			writeWithLength(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}

	}

	public void writeNullableJavaUTF8(String s) {
		if (s == null) {
			writeByte((byte) 0);
			return;
		}
		try {
			writeWithLengthNullable(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}

	private void writeWithLengthNullable(byte[] bytes) {
		writeVarInt(bytes.length + 1);
		write(bytes);
	}

	public void writeUTF8(String s) {
		int length = s.length();
		writeVarInt(length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[pos++] = (byte) c;
			} else {
				writeUtfChar(c);
			}
		}
	}

	public void writeNullableUTF8(String s) {
		if (s == null) {
			writeByte((byte) 0);
			return;
		}
		int length = s.length();
		writeVarInt(length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
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

	public final void writeWithLength(byte[] bytes) {
		writeVarInt(bytes.length);
		write(bytes);
	}

	public final void writeUTF16(String s) {
		int length = s.length();
		writeVarInt(length);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			writeByte((byte) (v >>> 8));
			writeByte((byte) (v));
		}
	}

	public final void writeNullableUTF16(String s) {
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
		return "[pos: " + pos + ", buf" + (buf.length < 100 ? ": " + Arrays.toString(buf) : " size " + buf.length) + "]";
	}
}
