/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.util.Recyclable;
import io.datakernel.util.Sliceable;
import io.datakernel.util.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.Math.min;

@SuppressWarnings({"WeakerAccess", "DefaultAnnotationParam", "unused"})
public class ByteBuf implements Recyclable, Sliceable<ByteBuf>, AutoCloseable {
	static final class ByteBufSlice extends ByteBuf {
		@NotNull
		private final ByteBuf root;

		private ByteBufSlice(@NotNull ByteBuf buf, int readPosition, int writePosition) {
			super(buf.array, readPosition, writePosition);
			this.root = buf;
		}

		@Override
		public void recycle() {
			root.recycle();
		}

		@Override
		public void addRef() {
			root.addRef();
		}

		@Override
		@NotNull
		public ByteBuf slice(int offset, int length) {
			return root.slice(offset, length);
		}

		@Override
		@Contract(pure = true)
		protected boolean isRecycled() {
			return root.isRecycled();
		}

		@Override
		@Contract(pure = true)
		protected boolean isRecycleNeeded() {
			return root.isRecycleNeeded();
		}
	}

	@NotNull
	protected final byte[] array;

	private int readPosition;
	private int writePosition;

	int refs;

	@Nullable
	ByteBuf next;

	private static final ByteBuf EMPTY = wrap(new byte[0], 0, 0);

	// creators
	private ByteBuf(@NotNull byte[] array, int readPosition, int writePosition) {
		assert readPosition >= 0 && readPosition <= writePosition && writePosition <= array.length
				: "Wrong ByteBuf boundaries - readPos: " + readPosition + ", writePos: " + writePosition + ", array.length: " + array.length;
		this.array = array;
		this.readPosition = readPosition;
		this.writePosition = writePosition;
	}

	@Contract(pure = true)
	public static ByteBuf empty() {
		assert EMPTY.readPosition == 0;
		assert EMPTY.writePosition == 0;
		return EMPTY;
	}

	@NotNull
	@Contract("_ -> new")
	public static ByteBuf wrapForWriting(@NotNull byte[] bytes) {
		return wrap(bytes, 0, 0);
	}

	@NotNull
	@Contract("_ -> new")
	public static ByteBuf wrapForReading(@NotNull byte[] bytes) {
		return wrap(bytes, 0, bytes.length);
	}

	@NotNull
	@Contract("_, _, _ -> new")
	public static ByteBuf wrap(@NotNull byte[] bytes, int readPosition, int writePosition) {
		return new ByteBuf(bytes, readPosition, writePosition);
	}

	// slicing
	@NotNull
	@Contract("-> new")
	@Override
	public ByteBuf slice() {
		return slice(readPosition, readRemaining());
	}

	@NotNull
	@Contract("_ -> new")
	public ByteBuf slice(int length) {
		return slice(readPosition, length);
	}

	@NotNull
	@Contract("_, _ -> new")
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

	public void addRef() {
		refs++;
	}

	@Override
	public void close() {
		recycle();
	}

	@Contract(pure = true)
	protected boolean isRecycled() {
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

	@Contract(pure = true)
	protected boolean isRecycleNeeded() {
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
		readPosition = byteBuffer.position();
	}

	public void ofWriteByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == array.length;
		writePosition = byteBuffer.position();
	}

	// getters & setters

	@NotNull
	@Contract(pure = true)
	public byte[] array() {
		return array;
	}

	@Contract(pure = true)
	public int readPosition() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return readPosition;
	}

	@Contract(pure = true)
	public int writePosition() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition;
	}

	@Contract(pure = true)
	public int limit() {
		return array.length;
	}

	public void readPosition(int pos) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert pos <= writePosition;
		readPosition = pos;
	}

	public void writePosition(int pos) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert pos >= readPosition && pos <= array.length;
		writePosition = pos;
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

	@Contract(pure = true)
	public int writeRemaining() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array.length - writePosition;
	}

	@Contract(pure = true)
	public int readRemaining() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition - readPosition;
	}

	@Contract(pure = true)
	public boolean canWrite() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition != array.length;
	}

	@Contract(pure = true)
	public boolean canRead() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return readPosition != writePosition;
	}

	public byte get() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readPosition < writePosition;
		return array[readPosition++];
	}

	@Contract(pure = true)
	public byte at(int index) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[index];
	}

	@Contract(pure = true)
	public byte peek() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[readPosition];
	}

	@Contract(pure = true)
	public byte peek(int offset) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert (readPosition + offset) < writePosition;
		return array[readPosition + offset];
	}

	public int drainTo(@NotNull byte[] array, int offset, int length) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert length >= 0 && (offset + length) <= array.length;
		assert readPosition + length <= writePosition;
		System.arraycopy(this.array, readPosition, array, offset, length);
		readPosition += length;
		return length;
	}

	public int drainTo(@NotNull ByteBuf buf, int length) {
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

	public void put(@NotNull ByteBuf buf) {
		put(buf.array, buf.readPosition, buf.writePosition - buf.readPosition);
		buf.readPosition = buf.writePosition;
	}

	public void put(@NotNull byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	public void put(@NotNull byte[] bytes, int offset, int length) {
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

	public int find(@NotNull byte[] bytes) {
		return find(bytes, 0, bytes.length);
	}

	public int find(@NotNull byte[] bytes, int off, int len) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		L:
		for (int pos = readPosition; pos <= writePosition - len; pos++) {
			for (int i = 0; i < len; i++) {
				if (array[pos + i] != bytes[off + i]) {
					continue L;
				}
			}
			return pos;
		}
		return -1;
	}

	@Contract(pure = true)
	public boolean isContentEqual(@NotNull byte[] array, int offset, int length) {
		return Utils.arraysEquals(this.array, readPosition, readRemaining(), array, offset, length);
	}

	@Contract(pure = true)
	public boolean isContentEqual(@NotNull ByteBuf other) {
		return isContentEqual(other.array, other.readPosition, other.readRemaining());
	}

	@Contract(pure = true)
	public boolean isContentEqual(byte[] array) {
		return isContentEqual(array, 0, array.length);
	}

	@Contract(pure = true)
	@NotNull
	public byte[] getArray() {
		byte[] bytes = new byte[readRemaining()];
		System.arraycopy(array, readPosition, bytes, 0, bytes.length);
		return bytes;
	}

	@Contract(pure = false)
	@NotNull
	public byte[] asArray() {
		byte[] bytes = getArray();
		recycle();
		return bytes;
	}

	@Contract(pure = true)
	public String getString(@NotNull Charset charset) {
		return new String(array, readPosition, readRemaining(), charset);
	}

	@Contract(pure = false)
	public String asString(@NotNull Charset charset) {
		String string = getString(charset);
		recycle();
		return string;
	}

	// region serialization input
	public int read(@NotNull byte[] b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return read(b, 0, b.length);
	}

	public int read(@NotNull byte[] b, int off, int len) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return drainTo(b, off, len);
	}

	public byte readByte() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[readPosition++];
	}

	public boolean readBoolean() {
		return readByte() != 0;
	}

	public char readChar() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		char c = (char) (((array[readPosition] & 0xFF) << 8) | (array[readPosition + 1] & 0xFF));
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
		byte b = array[readPosition];
		if (b >= 0) {
			result = b;
			readPosition += 1;
		} else {
			result = b & 0x7f;
			if ((b = array[readPosition + 1]) >= 0) {
				result |= b << 7;
				readPosition += 2;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = array[readPosition + 2]) >= 0) {
					result |= b << 14;
					readPosition += 3;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = array[readPosition + 3]) >= 0) {
						result |= b << 21;
						readPosition += 4;
					} else {
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
		long result = ((long) array[readPosition] << 56)
				| ((long) (array[readPosition + 1] & 0xFF) << 48)
				| ((long) (array[readPosition + 2] & 0xFF) << 40)
				| ((long) (array[readPosition + 3] & 0xFF) << 32)
				| ((long) (array[readPosition + 4] & 0xFF) << 24)
				| ((array[readPosition + 5] & 0xFF) << 16)
				| ((array[readPosition + 6] & 0xFF) << 8)
				| (array[readPosition + 7] & 0xFF);
		readPosition += 8;
		return result;
	}

	public short readShort() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		short result = (short) (((array[readPosition] & 0xFF) << 8)
				| (array[readPosition + 1] & 0xFF));
		readPosition += 2;
		return result;
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
	// endregion

	// region serialization output
	public void write(@NotNull byte[] b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		write(b, 0, b.length);
	}

	public void write(@NotNull byte[] b, int off, int len) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		System.arraycopy(b, off, array, writePosition, len);
		writePosition = writePosition + len;
	}

	public void writeBoolean(boolean v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		writeByte(v ? (byte) 1 : 0);
	}

	public void writeByte(byte v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		array[writePosition] = v;
		writePosition = writePosition + 1;
	}

	public void writeChar(char v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		array[writePosition] = (byte) (v >>> 8);
		array[writePosition] = (byte) v;
		writePosition = writePosition + 2;
	}

	public void writeDouble(double v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		writeLong(Double.doubleToLongBits(v));
	}

	public void writeFloat(float v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		writeInt(Float.floatToIntBits(v));
	}

	public void writeInt(int v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		array[writePosition] = (byte) (v >>> 24);
		array[writePosition + 1] = (byte) (v >>> 16);
		array[writePosition + 2] = (byte) (v >>> 8);
		array[writePosition + 3] = (byte) v;
		writePosition = writePosition + 4;
	}

	public void writeLong(long v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		int high = (int) (v >>> 32);
		int low = (int) v;
		array[writePosition] = (byte) (high >>> 24);
		array[writePosition + 1] = (byte) (high >>> 16);
		array[writePosition + 2] = (byte) (high >>> 8);
		array[writePosition + 3] = (byte) high;
		array[writePosition + 4] = (byte) (low >>> 24);
		array[writePosition + 5] = (byte) (low >>> 16);
		array[writePosition + 6] = (byte) (low >>> 8);
		array[writePosition + 7] = (byte) low;
		writePosition = writePosition + 8;
	}

	public void writeShort(short v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		array[writePosition] = (byte) (v >>> 8);
		array[writePosition + 1] = (byte) v;
		writePosition = writePosition + 2;
	}

	public void writeVarInt(int v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		if ((v & ~0x7F) == 0) {
			array[writePosition] = (byte) v;
			writePosition += 1;
			return;
		}
		array[writePosition] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[writePosition + 1] = (byte) v;
			writePosition += 2;
			return;
		}
		array[writePosition + 1] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[writePosition + 2] = (byte) v;
			writePosition += 3;
			return;
		}
		array[writePosition + 2] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[writePosition + 3] = (byte) v;
			writePosition += 4;
			return;
		}
		array[writePosition + 3] = (byte) (v | 0x80);
		v >>>= 7;
		array[writePosition + 4] = (byte) v;
		writePosition += 5;
	}

	public void writeVarLong(long v) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		if ((v & ~0x7F) == 0) {
			array[writePosition] = (byte) v;
			writePosition += 1;
			return;
		}
		array[writePosition] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[writePosition + 1] = (byte) v;
			writePosition += 2;
			return;
		}
		array[writePosition + 1] = (byte) (v | 0x80);
		v >>>= 7;
		writePosition += 2;
		for (; ; ) {
			if ((v & ~0x7FL) == 0) {
				writeByte((byte) v);
				return;
			} else {
				writeByte((byte) (v | 0x80));
				v >>>= 7;
			}
		}
	}

	// endregion

	@Override
	@Contract(pure = true)
	public String toString() {
		char[] chars = new char[min(readRemaining(), 256)];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[readPosition + i];
			chars[i] = (b == '\n') ? (char) 9166 : (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}
