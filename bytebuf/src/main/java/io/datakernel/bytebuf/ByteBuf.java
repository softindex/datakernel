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

package io.datakernel.bytebuf;

import io.datakernel.annotation.Nullable;
import io.datakernel.util.Recyclable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteBuf implements Recyclable, AutoCloseable {
	static final class ByteBufSlice extends ByteBuf {
		private ByteBuf root;

		private ByteBufSlice(ByteBuf buf, int readPosition, int writePosition) {
			super(buf.array, readPosition, writePosition);
			this.root = buf;
		}

		@Override
		public void recycle() {
			root.recycle();
		}

		@Override
		public ByteBuf slice(int offset, int length) {
			return root.slice(offset, length);
		}

		@Override
		boolean isRecycled() {
			return root.isRecycled();
		}

		@Override
		public boolean isRecycleNeeded() {
			return root.isRecycleNeeded();
		}
	}

	protected final byte[] array;

	private int readPosition;
	private int writePosition;

	int refs;

	private static final ByteBuf EMPTY = wrap(new byte[0], 0, 0);

	// creators
	private ByteBuf(byte[] array, int readPosition, int writePosition) {
		assert readPosition >= 0 && readPosition <= writePosition && writePosition <= array.length
				: "Wrong ByteBuf boundaries - readPos: " + readPosition + ", writePos: " + writePosition + ", array.length: " + array.length;
		this.array = array;
		this.readPosition = readPosition;
		this.writePosition = writePosition;
	}

	public static ByteBuf empty() {
		assert EMPTY.readPosition == 0;
		assert EMPTY.writePosition == 0;
		return EMPTY;
	}

	public static ByteBuf wrapForWriting(byte[] bytes) {
		return wrap(bytes, 0, 0);
	}

	public static ByteBuf wrapForReading(byte[] bytes) {
		return wrap(bytes, 0, bytes.length);
	}

	public static ByteBuf wrap(byte[] bytes, int readPosition, int writePosition) {
		return new ByteBuf(bytes, readPosition, writePosition);
	}

	// slicing
	public ByteBuf slice() {
		return slice(readPosition, readRemaining());
	}

	public ByteBuf slice(int length) {
		return slice(readPosition, length);
	}

	public ByteBuf slice(int offset, int length) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		if (!isRecycleNeeded()) {
			return ByteBuf.wrap(array, offset, offset + length);
		}
		refs++;
		return new ByteBufSlice(this, offset, offset + length);
	}

	// recycling
	@Override
	public void recycle() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		if (refs > 0 && --refs == 0) {
			//noinspection AssertWithSideEffects
			assert --refs == -1;
			ByteBufPool.recycle(this);
		}
	}

	@Override
	public void close() {
		recycle();
	}

	boolean isRecycled() {
		return refs == -1;
	}

	void reset() {
		assert isRecycled();
		refs = 1;
		rewind();
	}

	public void rewind() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		writePosition = 0;
		readPosition = 0;
	}

	public boolean isRecycleNeeded() {
		return refs > 0;
	}

	// byte buffers
	public ByteBuffer toReadByteBuffer() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return ByteBuffer.wrap(array, readPosition, readRemaining());
	}

	public ByteBuffer toWriteByteBuffer() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return ByteBuffer.wrap(array, writePosition, writeRemaining());
	}

	public void ofReadByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == writePosition;
		this.readPosition = byteBuffer.position();
	}

	public void ofWriteByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == array.length;
		this.writePosition = byteBuffer.position();
	}

	// getters & setters

	public byte[] array() {
		return array;
	}

	public int readPosition() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return readPosition;
	}

	public int writePosition() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition;
	}

	public int limit() {
		return array.length;
	}

	public void readPosition(int pos) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert pos <= writePosition;
		this.readPosition = pos;
	}

	public void writePosition(int pos) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert pos >= readPosition && pos <= array.length;
		this.writePosition = pos;
	}

	public void moveReadPosition(int delta) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readPosition + delta >= 0;
		assert readPosition + delta <= writePosition;
		readPosition += delta;
	}

	public void moveWritePosition(int delta) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert writePosition + delta >= readPosition;
		assert writePosition + delta <= array.length;
		writePosition += delta;
	}

	public int writeRemaining() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array.length - writePosition;
	}

	public int readRemaining() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition - readPosition;
	}

	public boolean canWrite() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition != array.length;
	}

	public boolean canRead() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return readPosition != writePosition;
	}

	public byte get() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readPosition < writePosition;
		return array[readPosition++];
	}

	public byte at(int index) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[index];
	}

	public byte peek() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[readPosition];
	}

	public byte peek(int offset) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert (readPosition + offset) < writePosition;
		return array[readPosition + offset];
	}

	public int drainTo(byte[] array, int offset, int length) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert length >= 0 && (offset + length) <= array.length;
		assert this.readPosition + length <= this.writePosition;
		System.arraycopy(this.array, this.readPosition, array, offset, length);
		this.readPosition += length;
		return length;
	}

	public int drainTo(ByteBuf buf, int length) {
		assert !buf.isRecycled();
		assert buf.writePosition + length <= buf.array.length;
		drainTo(buf.array, buf.writePosition, length);
		buf.writePosition += length;
		return length;
	}

	public void set(int index, byte b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		array[index] = b;
	}

	public void put(byte b) {
		set(writePosition, b);
		writePosition++;
	}

	public void put(ByteBuf buf) {
		put(buf.array, buf.readPosition, buf.writePosition - buf.readPosition);
		buf.readPosition = buf.writePosition;
	}

	public void put(byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	public void put(byte[] bytes, int offset, int length) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert writePosition + length <= array.length;
		assert offset + length <= bytes.length;
		System.arraycopy(bytes, offset, array, writePosition, length);
		writePosition += length;
	}

	public int find(byte b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		for (int i = readPosition; i < writePosition; i++) {
			if (array[i] == b) return i;
		}
		return -1;
	}

	public int find(byte[] bytes) {
		return find(bytes, 0, bytes.length);
	}

	public int find(byte[] bytes, int off, int len) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		L:
		for (int pos = readPosition; pos < writePosition - len; pos++) {
			for (int i = 0; i < len; i++) {
				if (array[pos + i] != bytes[off + i]) {
					continue L;
				}
			}
			return pos;
		}
		return -1;
	}

	public byte[] getArray() {
		byte[] bytes = new byte[readRemaining()];
		System.arraycopy(array, readPosition, bytes, 0, bytes.length);
		return bytes;
	}

	public byte[] asArray(){
		byte[] bytes = getArray();
		recycle();
		return bytes;
	}

	public String getString(Charset charset) {
		return new String(array, readPosition, readRemaining(), charset);
	}


	public String asString(Charset charset){
		String string = getString(charset);
		recycle();
		return string;
	}

	// region serialization input
	public int read(byte[] b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		return drainTo(b, off, len);
	}

	public byte readByte() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readRemaining() >= 1;

		return array[readPosition++];
	}

	public boolean readBoolean() {
		return readByte() != 0;
	}

	public char readChar() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readRemaining() >= 2;

		char c = (char) (((array[readPosition] & 0xFF) << 8) | ((array[readPosition + 1] & 0xFF)));
		readPosition += 2;
		return c;
	}

	public double readDouble() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		return Double.longBitsToDouble(readLong());
	}

	public float readFloat() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		return Float.intBitsToFloat(readInt());
	}

	public int readInt() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readRemaining() >= 4;

		int result = ((array[readPosition] & 0xFF) << 24)
				| ((array[readPosition + 1] & 0xFF) << 16)
				| ((array[readPosition + 2] & 0xFF) << 8)
				| (array[readPosition + 3] & 0xFF);
		readPosition += 4;
		return result;
	}

	public int readVarInt() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int result;
		assert readRemaining() >= 1;
		byte b = array[readPosition];
		if (b >= 0) {
			result = b;
			readPosition += 1;
		} else {
			assert readRemaining() >= 2;
			result = b & 0x7f;
			if ((b = array[readPosition + 1]) >= 0) {
				result |= b << 7;
				readPosition += 2;
			} else {
				assert readRemaining() >= 3;
				result |= (b & 0x7f) << 7;
				if ((b = array[readPosition + 2]) >= 0) {
					result |= b << 14;
					readPosition += 3;
				} else {
					assert readRemaining() >= 4;
					result |= (b & 0x7f) << 14;
					if ((b = array[readPosition + 3]) >= 0) {
						result |= b << 21;
						readPosition += 4;
					} else {
						assert readRemaining() >= 5;
						result |= (b & 0x7f) << 21;
						if ((b = array[readPosition + 4]) >= 0) {
							result |= b << 28;
							readPosition += 5;
						} else
							throw new IllegalArgumentException();
					}
				}
			}
		}
		return result;
	}

	public long readLong() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readRemaining() >= 8;

		long result = ((long) array[readPosition] << 56)
				| ((long) (array[readPosition + 1] & 0xFF) << 48)
				| ((long) (array[readPosition + 2] & 0xFF) << 40)
				| ((long) (array[readPosition + 3] & 0xFF) << 32)
				| ((long) (array[readPosition + 4] & 0xFF) << 24)
				| ((array[readPosition + 5] & 0xFF) << 16)
				| ((array[readPosition + 6] & 0xFF) << 8)
				| ((array[readPosition + 7] & 0xFF));

		readPosition += 8;
		return result;
	}

	public short readShort() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readRemaining() >= 2;

		short result = (short) (((array[readPosition] & 0xFF) << 8)
				| ((array[readPosition + 1] & 0xFF)));
		readPosition += 2;
		return result;
	}

	public String readIso88591() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		return doReadIso88591(length);
	}

	@Nullable
	public String readIso88591Nullable() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadIso88591(length - 1);
	}

	private String doReadIso88591(int length) {
		if (length == 0)
			return "";
		if (length > readRemaining())
			throw new IllegalArgumentException();

		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			chars[i] = (char) c;
		}
		return new String(chars, 0, length);
	}

	@Deprecated
	public String readCustomUTF8() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		return doReadCustomUTF8(length);
	}

	@Nullable
	@Deprecated
	public String readCustomUTF8Nullable() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadCustomUTF8(length - 1);
	}

	@Deprecated
	private String doReadCustomUTF8(int length) {
		if (length == 0)
			return "";
		if (length > readRemaining())
			throw new IllegalArgumentException();
		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			if (c < 0x80) {
				chars[i] = (char) c;
			} else if (c < 0xE0) {
				chars[i] = (char) ((c & 0x1F) << 6 | readByte() & 0x3F);
			} else {
				chars[i] = (char) ((c & 0x0F) << 12 | (readByte() & 0x3F) << 6 | (readByte() & 0x3F));
			}
		}
		return new String(chars, 0, length);
	}

	public String readUTF16() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		return doReadUTF16(length);
	}

	@Nullable
	public String readUTF16Nullable() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF16(length - 1);
	}

	private String doReadUTF16(int length) {
		if (length == 0)
			return "";
		if (length * 2 > readRemaining())
			throw new IllegalArgumentException();

		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readByte() << 8) + (readByte()));
		}
		return new String(chars, 0, length);
	}

	public long readVarLong() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte();
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		throw new IllegalArgumentException();
	}

	public String readJavaUTF8() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		return doReadJavaUTF8(length);
	}

	@Nullable
	public String readJavaUTF8Nullable() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";

		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadJavaUTF8(length - 1);
	}

	private String doReadJavaUTF8(int length) {
		if (length == 0)
			return "";
		if (length > readRemaining())
			throw new IllegalArgumentException();
		readPosition += length;

		return new String(array, readPosition - length, length, UTF_8);
	}
	// endregion

	// region serialization output
	public void write(byte[] b) {
		writePosition = SerializationUtils.write(array, writePosition, b);
	}

	public void write(byte[] b, int off, int len) {
		writePosition = SerializationUtils.write(array, writePosition, b, off, len);
	}

	public void writeBoolean(boolean v) {
		writePosition = SerializationUtils.writeBoolean(array, writePosition, v);
	}

	public void writeByte(byte v) {
		writePosition = SerializationUtils.writeByte(array, writePosition, v);
	}

	public void writeChar(char v) {
		writePosition = SerializationUtils.writeChar(array, writePosition, v);
	}

	public void writeDouble(double v) {
		writePosition = SerializationUtils.writeDouble(array, writePosition, v);
	}

	public void writeFloat(float v) {
		writePosition = SerializationUtils.writeFloat(array, writePosition, v);
	}

	public void writeInt(int v) {
		writePosition = SerializationUtils.writeInt(array, writePosition, v);
	}

	public void writeLong(long v) {
		writePosition = SerializationUtils.writeLong(array, writePosition, v);
	}

	public void writeShort(short v) {
		writePosition = SerializationUtils.writeShort(array, writePosition, v);
	}

	public void writeVarInt(int v) {
		writePosition = SerializationUtils.writeVarInt(array, writePosition, v);
	}

	public void writeVarLong(long v) {
		writePosition = SerializationUtils.writeVarLong(array, writePosition, v);
	}

	public void writeIso88591(String s) {
		writePosition = SerializationUtils.writeIso88591(array, writePosition, s);
	}

	public void writeIso88591Nullable(String s) {
		writePosition = SerializationUtils.writeIso88591Nullable(array, writePosition, s);
	}

	public void writeJavaUTF8(String s) {
		writePosition = SerializationUtils.writeJavaUTF8(array, writePosition, s);
	}

	public void writeJavaUTF8Nullable(String s) {
		writePosition = SerializationUtils.writeJavaUTF8Nullable(array, writePosition, s);
	}

	@Deprecated
	public void writeCustomUTF8(String s) {
		writePosition = SerializationUtils.writeCustomUTF8(array, writePosition, s);
	}

	@Deprecated
	public void writeCustomUTF8Nullable(String s) {
		writePosition = SerializationUtils.writeCustomUTF8Nullable(array, writePosition, s);
	}

	public final void writeUTF16(String s) {
		writePosition = SerializationUtils.writeUTF16(array, writePosition, s);
	}

	public final void writeUTF16Nullable(String s) {
		writePosition = SerializationUtils.writeUTF16Nullable(array, writePosition, s);
	}
	// endregion

	@Override
	public boolean equals(Object o) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		if (this == o) return true;
		if (o == null || !(ByteBuf.class == o.getClass() || ByteBufSlice.class == o.getClass())) return false;
		ByteBuf other = (ByteBuf) o;
		return readRemaining() == other.readRemaining() &&
				arraysEquals(this.array, this.readPosition, this.writePosition, other.array, other.readPosition);
	}

	private static boolean arraysEquals(byte[] array, int offset, int limit, byte[] arr, int off) {
		for (int i = 0; i < limit - offset; i++) {
			if (array[offset + i] != arr[off + i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		int result = 1;
		for (int i = readPosition; i < writePosition; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	@Override
	public String toString() {
		char[] chars = new char[min(readRemaining(), 256)];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[readPosition + i];
			chars[i] = (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}

}
