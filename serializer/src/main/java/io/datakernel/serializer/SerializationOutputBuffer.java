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

	protected static void ensureSize(int size) {
	}

	public static int write(byte[] from, byte[] to, int offTo) {
		return write(from, 0, to, offTo, from.length);
	}

	public static int write(byte[] from, int offFrom, byte[] to, int offTo, int len) {
		ensureSize(len);
		System.arraycopy(from, offFrom, to, offTo, len);
		return offTo + len;
	}

	public static int writeBoolean(byte[] buf, int off, boolean v) {
		return writeByte(buf, off, v ? (byte) 1 : 0);
	}

	public static int writeByte(byte[] buf, int off, byte v) {
		ensureSize(1);
		buf[off] = v;
		return off + 1;
	}

	public static int writeChar(byte[] buf, int off, char v) {
		ensureSize(2);
		writeByte(buf, off, (byte) (v >>> 8));
		writeByte(buf, off + 1, (byte) (v));
		return off + 2;
	}

	public static int writeDouble(byte[] buf, int off, double v) {
		return writeLong(buf, off, Double.doubleToLongBits(v));
	}

	public static int writeFloat(byte[] buf, int off, float v) {
		return writeInt(buf, off, Float.floatToIntBits(v));
	}

	public static int writeInt(byte[] buf, int off, int v) {
		ensureSize(4);
		buf[off] = (byte) (v >>> 24);
		buf[off + 1] = (byte) (v >>> 16);
		buf[off + 2] = (byte) (v >>> 8);
		buf[off + 3] = (byte) (v);
		return off + 4;
	}

	public static int writeLong(byte[] buf, int off, long v) {
		ensureSize(8);
		int high = (int) (v >>> 32);
		int low = (int) v;
		buf[off] = (byte) (high >>> 24);
		buf[off + 1] = (byte) (high >>> 16);
		buf[off + 2] = (byte) (high >>> 8);
		buf[off + 3] = (byte) high;
		buf[off + 4] = (byte) (low >>> 24);
		buf[off + 5] = (byte) (low >>> 16);
		buf[off + 6] = (byte) (low >>> 8);
		buf[off + 7] = (byte) low;
		return off + 8;
	}

	public static int writeShort(byte[] buf, int off, short v) {
		ensureSize(2);
		buf[off] = (byte) (v >>> 8);
		buf[off + 1] = (byte) (v);
		return off + 2;
	}

	public static int writeVarInt(byte[] buf, int off, int v) {
		ensureSize(5);
		if ((v & ~0x7F) == 0) {
			buf[off] = (byte) v;
			return off + 1;
		}
		buf[off] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[off + 1] = (byte) v;
			return off + 2;
		}
		buf[off + 1] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[off + 2] = (byte) v;
			return off + 3;
		}
		buf[off + 2] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[off + 3] = (byte) v;
			return off + 4;
		}
		buf[off + 3] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		buf[off + 4] = (byte) v;
		return off + 5;
	}

	public static int writeVarLong(byte[] buf, int off, long v) {
		ensureSize(9);
		if ((v & ~0x7F) == 0) {
			return writeByte(buf, off, (byte) v);
		} else {
			off = writeByte(buf, off, (byte) ((v & 0x7F) | 0x80));
			v >>>= 7;
			if ((v & ~0x7F) == 0) {
				off = writeByte(buf, off, (byte) v);
			} else {
				off = writeByte(buf, off, (byte) ((v & 0x7F) | 0x80));
				v >>>= 7;
				if ((v & ~0x7F) == 0) {
					off = writeByte(buf, off, (byte) v);
				} else {
					off = writeByte(buf, off, (byte) ((v & 0x7F) | 0x80));
					v >>>= 7;
					if ((v & ~0x7F) == 0) {
						off = writeByte(buf, off, (byte) v);
					} else {
						off = writeByte(buf, off, (byte) ((v & 0x7F) | 0x80));
						v >>>= 7;
						if ((v & ~0x7F) == 0) {
							off = writeByte(buf, off, (byte) v);
						} else {
							off = writeByte(buf, off, (byte) ((v & 0x7F) | 0x80));
							v >>>= 7;
							if ((v & ~0x7F) == 0) {
								off = writeByte(buf, off, (byte) v);
							} else {
								off = writeByte(buf, off, (byte) ((v & 0x7F) | 0x80));
								v >>>= 7;

								for (; ; ) {
									if ((v & ~0x7FL) == 0) {
										off = writeByte(buf, off, (byte) v);
										return off;
									} else {
										off = writeByte(buf, off, (byte) (((int) v & 0x7F) | 0x80));
										v >>>= 7;
									}
								}
							}
						}
					}
				}
			}
		}
		return off;
	}

	public static int writeIso88591(byte[] buf, int off, String s) {
		int length = s.length();
		off = writeVarInt(buf, off, length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			buf[off++] = (byte) c;
		}
		return off;
	}

	public static int writeNullableIso88591(byte[] buf, int off, String s) {
		if (s == null) {
			return writeByte(buf, off, (byte) 0);
		}
		int length = s.length();
		off = writeVarInt(buf, off, length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			buf[off++] = (byte) c;
		}
		return off;
	}

	public static int writeJavaUTF8(byte[] buf, int off, String s) {
		try {
			return writeWithLength(buf, off, s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}

	}

	public static int writeNullableJavaUTF8(byte[] buf, int off, String s) {
		if (s == null) {
			return writeByte(buf, off, (byte) 0);
		}
		try {
			return writeWithLengthNullable(buf, off, s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}

	private static int writeWithLengthNullable(byte[] buf, int off, byte[] bytes) {
		off = writeVarInt(buf, off, bytes.length + 1);
		return write(bytes, buf, off);
	}

	public static int writeUTF8(byte[] buf, int off, String s) {
		int length = s.length();
		off = writeVarInt(buf, off, length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[off++] = (byte) c;
			} else {
				off = writeUtfChar(buf, off, c);
			}
		}
		return off;
	}

	public static int writeNullableUTF8(byte[] buf, int off, String s) {
		if (s == null) {
			return writeByte(buf, off, (byte) 0);
		}
		int length = s.length();
		off = writeVarInt(buf, off, length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[off++] = (byte) c;
			} else {
				off = writeUtfChar(buf, off, c);
			}
		}
		return off;
	}

	private static int writeUtfChar(byte[] buf, int off, int c) {
		if (c <= 0x07FF) {
			buf[off] = (byte) (0xC0 | c >> 6 & 0x1F);
			buf[off + 1] = (byte) (0x80 | c & 0x3F);
			return off + 2;
		} else {
			buf[off] = (byte) (0xE0 | c >> 12 & 0x0F);
			buf[off + 1] = (byte) (0x80 | c >> 6 & 0x3F);
			buf[off + 2] = (byte) (0x80 | c & 0x3F);
			return off + 3;
		}
	}

	public static int writeWithLength(byte[] buf, int off, byte[] bytes) {
		off = writeVarInt(buf, off, bytes.length);
		return write(bytes, buf, off);
	}

	public static int writeUTF16(byte[] buf, int off, String s) {
		int length = s.length();
		off = writeVarInt(buf, off, length);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			off = writeByte(buf, off, (byte) (v >>> 8));
			off = writeByte(buf, off, (byte) (v));
		}
		return off;
	}

	public static int writeNullableUTF16(byte[] buf, int off, String s) {
		if (s == null) {
			return writeByte(buf, off, (byte) 0);
		}
		int length = s.length();
		off = writeVarInt(buf, off, length + 1);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			off = writeByte(buf, off, (byte) (v >>> 8));
			off = writeByte(buf, off, (byte) (v));
		}
		return off;
	}

	@Override
	public String toString() {
		return "[pos: " + pos + ", buf" + (buf.length < 100 ? ": " + Arrays.toString(buf) : " size " + buf.length) + "]";
	}
}
