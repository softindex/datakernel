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

/**
 * Represents a wrapper over a byte array and has 2 positions: {@link #readPosition} and
 * {@link #writePosition}.
 * <p>
 * When you write data to {@code ByteBuf}, it's {@link #writePosition} increases by the amount of bytes written.
 * <p>
 * When you read data from {@code ByteBuf}, it's {@link #readPosition} increases by the amount of bytes read.
 * <p>
 * You can read bytes from {@code ByteBuf} only when {@code writePosition} is bigger than {@code readPosition}.
 * <p>
 * You can write bytes to {@code ByteBuf} until {@code writePosition} doesn't exceed length of the underlying array.
 * <p>
 * ByteBuf is similar to a FIFO byte queue except it has no wrap-around or growth.
 */

@SuppressWarnings({"WeakerAccess", "DefaultAnnotationParam", "unused"})
public class ByteBuf implements Recyclable, Sliceable<ByteBuf>, AutoCloseable {

	/**
	 * Allows to create slices of {@link ByteBuf}, helper class.
	 * <p>
	 * A slice is a wrapper over an original {@code ByteBuf}. A slice links to the same byte array as the original
	 * {@code ByteBuf}.
	 * <p>
	 * You still have to recycle original {@code ByteBuf} as well as all of its slices.
	 */
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

	/** Stores bytes of this {@code ByteBuf}. */
	@NotNull
	protected final byte[] array;

	/** Stores <i>readPosition</i> of this {@code ByteBuf}. */
	private int readPosition;
	/** Stores <i>writePosition</i> of this {@code ByteBuf}. */
	private int writePosition;

	/**
	 * Shows whether this {@code ByteBuf} needs to be recycled.
	 * When you use {@link #slice()} this value increases by 1.
	 * When you use {@link #recycle()} this value decreases by 1.
	 * <p>
	 * This {@code ByteBuf} will be returned to the {@link ByteBufPool}
	 * only when this value equals 0.
	 */
	int refs;

	@Nullable
	ByteBuf next;

	/**
	 * Represents an empty ByteBuf with {@link #readPosition} and
	 * {@link #writePosition} set at value 0.
	 */
	private static final ByteBuf EMPTY = wrap(new byte[0], 0, 0);

	// creators
	/**
	 * Creates a {@code ByteBuf} with custom byte array, {@code writePosition} and {@code readPosition}.
	 *
	 * @param array byte array to be wrapped into {@code ByteBuf}
	 * @param readPosition value of {@code readPosition} of {@code ByteBuf}
	 * @param writePosition value of {@code writePosition} of {@code ByteBuf}
	 */
	private ByteBuf(@NotNull byte[] array, int readPosition, int writePosition) {
		assert readPosition >= 0 && readPosition <= writePosition && writePosition <= array.length
				: "Wrong ByteBuf boundaries - readPos: " + readPosition + ", writePos: " + writePosition + ", array.length: " + array.length;
		this.array = array;
		this.readPosition = readPosition;
		this.writePosition = writePosition;
	}

	/**
	 * Creates an empty {@code ByteBuf} with array of size 0,
	 * {@code writePosition} and {@code readPosition} both equal to 0.
	 *
	 * @return an empty {@code ByteBuf}
	 */
	@Contract(pure = true)
	public static ByteBuf empty() {
		assert EMPTY.readPosition == 0;
		assert EMPTY.writePosition == 0;
		return EMPTY;
	}

	/**
	 * Wraps provided byte array into {@code ByteBuf} with {@code writePosition} equal to 0.
	 *
	 * @param bytes byte array to be wrapped into {@code ByteBuf}
	 *
	 * @return {@code ByteBuf} over underlying byte array that is ready for writing
	 */
	@NotNull
	@Contract("_ -> new")
	public static ByteBuf wrapForWriting(@NotNull byte[] bytes) {
		return wrap(bytes, 0, 0);
	}

	/**
	 * Wraps provided byte array into {@code ByteBuf} with {@code writePosition} equal to length of provided array.
	 *
	 * @param bytes byte array to be wrapped into {@code ByteBuf}
	 * @return {@code ByteBuf} over underlying byte array that is ready for reading
	 */
	@NotNull
	@Contract("_ -> new")
	public static ByteBuf wrapForReading(@NotNull byte[] bytes) {
		return wrap(bytes, 0, bytes.length);
	}

	/**
	 * Wraps provided byte array into {@code ByteBuf} with
	 * specified {@code writePosition} and {@code readPosition}.
	 *
	 * @param bytes byte array to be wrapped into {@code ByteBuf}
	 * @param readPosition {@code readPosition} of {@code ByteBuf}
	 * @param writePosition {@code writePosition} of {@code ByteBuf}
	 * @return {@code ByteBuf} over underlying byte array with given {@code writePosition} and {@code readPosition}
	 */
	@NotNull
	@Contract("_, _, _ -> new")
	public static ByteBuf wrap(@NotNull byte[] bytes, int readPosition, int writePosition) {
		return new ByteBuf(bytes, readPosition, writePosition);
	}

	// slicing
	/**
	 * Creates a slice of this {@code ByteBuf} if it is not recycled.
	 * Its {@code readPosition} and {@code writePosition} won't change.
	 *
	 * {@link #refs} increases by 1.
	 *
	 * @return a {@link ByteBufSlice} of this {@code ByteBuf}
	 */
	@NotNull
	@Contract("-> new")
	@Override
	public ByteBuf slice() {
		return slice(readPosition, readRemaining());
	}

	/**
	 * Creates a slice of this {@code ByteBuf} with the given length if
	 * it is not recycled.
	 *
	 * @param length length of the new slice. Defines {@code writePosition}
	 *               of the new {@link ByteBufSlice}.
	 *               It is added to the current {@link #readPosition}.
	 * @return a {@code ByteBufSlice} of this {@code ByteBuf}.
	 */
	@NotNull
	@Contract("_ -> new")
	public ByteBuf slice(int length) {
		return slice(readPosition, length);
	}

	/**
	 * Creates a slice of this {@code ByteBuf} with the given offset and length.
	 *
	 * @param offset offset from which to slice this {@code ByteBuf}.
	 * @param length length of the slice.
	 * @return a {@code ByteBufSlice} of this {@code ByteBuf} with the given offset and length.
	 */
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
	/**
	 * Recycles this {@code ByteBuf} by returning it to {@link ByteBufPool}.
	 */
	@Override
	public void recycle() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		if (refs > 0 && --refs == 0) {
			//noinspection AssertWithSideEffects
			assert --refs == -1;
			ByteBufPool.recycle(this);
		}
	}

	/**
	 * Increases {@link #refs} value by 1.
	 */
	public void addRef() {
		refs++;
	}

	/**
	 * Recycles this {@code ByteBuf} by returning it to {@link ByteBufPool}.
	 */
	@Override
	public void close() {
		recycle();
	}

	/**
	 * Checks if this {@code ByteBuf} is recycled.
	 *
	 * @return {@code true} or {@code false}
	 */
	@Contract(pure = true)
	protected boolean isRecycled() {
		return refs == -1;
	}

	/**
	 * Sets {@code writePosition} and {@code readPosition} of this {@code ByteBuf} to 0 if it is recycled.
	 * Sets {@link #refs} to 1.
	 */
	void reset() {
		assert isRecycled();
		refs = 1;
		rewind();
	}

	/**
	 * Sets {@code writePosition} and {@code readPosition} of this {@code ByteBuf} to 0.
	 */
	public void rewind() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		writePosition = 0;
		readPosition = 0;
	}

	/**
	 * Checks if this {@code ByteBuf} needs recycling by checking the value of {@link #refs}.
	 * If the value is greater than 0, returns {@code true}.
	 *
	 * @return {@code true} if this {@code ByteBuf} needs recycle, otherwise {@code false}
	 */
	@Contract(pure = true)
	protected boolean isRecycleNeeded() {
		return refs > 0;
	}

	// byte buffers
	/**
	 * Wraps this {@code ByteBuf} into Java's {@link ByteBuffer} ready to read.
	 *
	 * @return {@link ByteBuffer} ready to read
	 */
	public ByteBuffer toReadByteBuffer() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return ByteBuffer.wrap(array, readPosition, readRemaining());
	}

	/**
	 * Wraps this {@code ByteBuf} into Java's {@link ByteBuffer} ready to write.
	 *
	 * @return {@link ByteBuffer} ready to write
	 */
	public ByteBuffer toWriteByteBuffer() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return ByteBuffer.wrap(array, writePosition, writeRemaining());
	}

	/**
	 * Unwraps given Java's {@link ByteBuffer} into {@code ByteBuf}.
	 *
	 * @param byteBuffer {@link ByteBuffer} to be unwrapped
	 */
	public void ofReadByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == writePosition;
		readPosition = byteBuffer.position();
	}

	/**
	 * Unwraps given Java's {@link ByteBuffer} into {@code ByteBuf}.
	 *
	 * @param byteBuffer {@link ByteBuffer} to be unwrapped
	 */
	public void ofWriteByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == array.length;
		writePosition = byteBuffer.position();
	}

	// getters & setters

	/**
	 * Returns byte array {@link #array}.
	 *
	 * @return {@link #array}
	 */
	@NotNull
	@Contract(pure = true)
	public byte[] array() {
		return array;
	}

	/**
	 * Returns {@code readPosition} if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@code readPosition}
	 */
	@Contract(pure = true)
	public int readPosition() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return readPosition;
	}

	/**
	 * Returns {@code writePosition} if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@code writePosition}
	 */
	@Contract(pure = true)
	public int writePosition() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition;
	}

	/**
	 * Returns length of the {@link #array} of this {@code ByteBuf}.
	 *
	 * @return length of this {@code ByteBuf}
	 */
	@Contract(pure = true)
	public int limit() {
		return array.length;
	}

	/**
	 * Sets {@code readPosition} if this {@code ByteBuf} is not recycled.
	 *
	 * @param pos the value which will be assigned to the {@code readPosition}.
	 *               Must be smaller or equal to {@code writePosition}
	 */
	public void readPosition(int pos) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert pos <= writePosition;
		readPosition = pos;
	}

	/**
	 * Sets {@link #writePosition} if this {@code ByteBuf} is not recycled.
	 *
	 * @param pos the value which will be assigned to the {@code writePosition}.
	 *               Must be bigger or equal to {@code readPosition}
	 *               and smaller than length of the {@link #array}
	 */
	public void writePosition(int pos) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert pos >= readPosition && pos <= array.length;
		writePosition = pos;
	}

	/**
	 * Sets new value of {@link #readPosition} by moving it by the given delta
	 * if this {@link ByteBuf} is not recycled.
	 *
	 * @param delta the value by which current {@link #readPosition} will be moved.
	 *              New {@link #readPosition} must be bigger or equal to 0
	 *              and smaller or equal to {@link #writePosition}
	 */
	public void moveReadPosition(int delta) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readPosition + delta >= 0;
		assert readPosition + delta <= writePosition;
		readPosition += delta;
	}

	/**
	 * Sets new value of {@code writePosition} by moving it by the given delta
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @param delta the value by which current {@code writePosition} will be moved.
	 *              New {@code writePosition} must be bigger or equal to {@code readPosition}
	 *              and smaller or equal to the length of the {@link #array}
	 */
	public void moveWritePosition(int delta) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert writePosition + delta >= readPosition;
		assert writePosition + delta <= array.length;
		writePosition += delta;
	}

	/**
	 * Returns the amount of bytes which are available for writing
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return amount of bytes available for writing
	 */
	@Contract(pure = true)
	public int writeRemaining() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array.length - writePosition;
	}

	/**
	 * Returns the amount of bytes which are available for reading
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return amount of bytes available for reading
	 */
	@Contract(pure = true)
	public int readRemaining() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition - readPosition;
	}

	/**
	 * Checks if there are bytes available for writing
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@code true} if {@code writePosition} doesn't equal the
	 * length of the {@link #array}, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean canWrite() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return writePosition != array.length;
	}

	/**
	 * Checks if there are bytes available for reading
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@code true} if {@code readPosition} doesn't equal
	 * {@code writePosition}, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean canRead() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return readPosition != writePosition;
	}

	/**
	 * Returns the byte from this {@code ByteBuf} which index is equal
	 * to {@code readPosition}. Then increases {@code readPosition} by 1.
	 *
	 * @return {@code byte} value at the {@link #readPosition}
	 */
	public byte get() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert readPosition < writePosition;
		return array[readPosition++];
	}

	/**
	 * Returns byte from this {@code ByteBuf} which index is equal
	 * to the passed value. Then increases {@code readPosition} by 1.
	 *
	 * @param  index index of the byte to be returned.
	 * @return the {@code byte} at the specified position.
	 */
	@Contract(pure = true)
	public byte at(int index) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[index];
	}

	/**
	 * Returns a {@code byte} from this {@link #array} which is
	 * located at {@code readPosition} if this {@code ByteBuf} is not recycled.
	 *
	 * @return a {@code byte} from this {@link #array} which is
	 * located at {@code readPosition}.
	 */
	@Contract(pure = true)
	public byte peek() {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		return array[readPosition];
	}

	/**
	 * Returns a {@code byte} from this {@link #array} which is
	 * located at {@code readPosition} position increased by the offset
	 * if this {@code ByteBuf} is not recycled. {@code readPosition} doesn't
	 * change.
	 *
	 * @param offset added to the {@code readPosition} value. Received value
	 *               must be smaller than current {@code writePosition}.
	 * @return a {@code byte} from this {@link #array} which is
	 * located at {@code readPosition} with provided offset
	 */
	@Contract(pure = true)
	public byte peek(int offset) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert (readPosition + offset) < writePosition;
		return array[readPosition + offset];
	}

	/**
	 * Drains bytes from this {@code ByteBuf} starting from current {@code readPosition}
	 * to a given byte array with specified offset and length.
	 *
	 * This {@code ByteBuf} must be not recycled.
	 *
	 * @param array array to which bytes will be drained to
	 * @param offset starting position in the destination data.
	 *               The sum of the value and the {@code length} parameter
	 *               must be smaller or equal to {@link #array} length
	 * @param length number of bytes to be drained to given array.
	 *               Must be greater or equal to 0.
	 *               The sum of the value and {@code readPosition}
	 *               must be smaller or equal to {@code writePosition}
	 * @return number of bytes that were drained.
	 */
	public int drainTo(@NotNull byte[] array, int offset, int length) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert length >= 0 && (offset + length) <= array.length;
		assert readPosition + length <= writePosition;
		System.arraycopy(this.array, readPosition, array, offset, length);
		readPosition += length;
		return length;
	}

	/**
	 * Drains bytes to a given {@code ByteBuf}.
	 *
	 * @see #drainTo(byte[], int, int)
	 */
	public int drainTo(@NotNull ByteBuf buf, int length) {
		assert !buf.isRecycled();
		assert buf.writePosition + length <= buf.array.length;
		drainTo(buf.array, buf.writePosition, length);
		buf.writePosition += length;
		return length;
	}

	/**
	 * Sets given {@code byte} at particular position of the
	 * {@link #array} if this {@code ByteBuf} is not recycled.
	 *
	 * @param index the index of the {@link #array} where
	 *                 the given {@code byte} will be set
	 * @param b the byte to be set at the given index of the {@link #array}
	 */
	public void set(int index, byte b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		array[index] = b;
	}

	/**
	 * Puts given {@code byte} to the {@link #array} at the {@code writePosition}
	 * and increases the {@code writePosition} by 1.
	 *
	 * @param b the byte which will be put to the {@link #array}.
	 */
	public void put(byte b) {
		set(writePosition, b);
		writePosition++;
	}

	/**
	 * Puts given ByteBuf to this {@code ByteBuf} from the {@code writePosition}
	 * and increases the {@code writePosition} by the length of the given ByteBuf.
	 * Then equates ByteBuf's {@code readPosition} to {@code writePosition}.
	 *
	 * Only those bytes which are located between {@code readPosition}
	 * and {@code writePosition} are put to the {@link #array}.
	 *
	 * @param buf the ByteBuf which will be put to the {@code ByteBuf}
	 */
	public void put(@NotNull ByteBuf buf) {
		put(buf.array, buf.readPosition, buf.writePosition - buf.readPosition);
		buf.readPosition = buf.writePosition;
	}


	/**
	 * Puts given byte array to the {@link #array} at the {@code writePosition}
	 * and increases the {@code writePosition} by the length of the given array.
	 *
	 * @param bytes the byte array which will be put to the {@link #array}
	 */
	public void put(@NotNull byte[] bytes) {
		put(bytes, 0, bytes.length);
	}


	/**
	 * Puts given byte array to the {@link ByteBuf} from the {@code writePosition}
	 * with given offset.
	 * Increases the {@code writePosition} by the length of the given array.
	 *
	 * This {@link ByteBuf} must be not recycled.
	 * Its length must be greater or equal to the sum of its {@code writePosition}
	 * and the length of the byte array which will be put in it.
	 * Also, the sum of the provided offset and length of the byte array which will
	 * be put to the {@link #array} must smaller or equal to the whole length of the byte array.
	 *
	 * @param bytes the byte array which will be put to the {@link #array}
	 * @param offset value of the offset in the byte array
	 * @param length length of the byte array which
	 *                  will be put to the {@link #array}
	 */
	public void put(@NotNull byte[] bytes, int offset, int length) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		assert writePosition + length <= array.length;
		assert offset + length <= bytes.length;
		System.arraycopy(bytes, offset, array, writePosition, length);
		writePosition += length;
	}

	/**
	 * Finds the given value in the {@link #array} and returns its position.
	 *
	 * This {@code ByteBuf} must be not recycled.
	 *
	 * @param b the {@code byte} which is to be found in the {@link #array}
	 * @return position of byte in the {@link #array}. If the byte wasn't found,
	 * returns -1
	 */
	public int find(byte b) {
		assert !isRecycled() : "Attempt to use recycled bytebuf";
		for (int i = readPosition; i < writePosition; i++) {
			if (array[i] == b) return i;
		}
		return -1;
	}


	/**
	 * Finds the given byte array in the {@link #array} and returns its position.
	 * This {@code ByteBuf} must be not recycled.
	 *
	 * @param bytes the byte array which is to be found in the {@link #array}
	 * @return the position of byte array in the {@link #array}. If the byte wasn't found,
	 * returns -1
	 */
	public int find(@NotNull byte[] bytes) {
		return find(bytes, 0, bytes.length);
	}

	/**
	 * Finds the given byte array in the {@link #array} and returns its position.
	 * This {@link ByteBuf} must be not recycled.
	 *
	 * @param bytes the byte array which is to be found in the {@link #array}
	 * @param off offset in the byte array
	 * @param len amount of the bytes to be found
	 * @return the position of byte array in the {@link #array}. If the byte wasn't found,
	 * returns -1
	 */
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

	/**
	 * Checks if provided array is equal to the readable bytes of the {@link #array}.
	 *
	 * @param array byte array to be compared with the {@link #array}
	 * @param offset offset value for the provided byte array
	 * @param length amount of the bytes to be compared
	 * @return {@code true} if the byte array is equal to the array, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean isContentEqual(@NotNull byte[] array, int offset, int length) {
		return Utils.arraysEquals(this.array, readPosition, readRemaining(), array, offset, length);
	}

	/**
	 * Checks if provided {@code ByteBuf} readable bytes are equal to the
	 * readable bytes of the {@link #array}.
	 *
	 * @param other {@code ByteBuf} to be compared with the {@link #array}
	 * @return {@code true} if the {@code ByteBuf} is equal to the array,
	 * otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean isContentEqual(@NotNull ByteBuf other) {
		return isContentEqual(other.array, other.readPosition, other.readRemaining());
	}

	/**
	 * Checks if provided array is equal to the readable bytes of the {@link #array}.
	 *
	 * @param array byte array to be compared with the {@link #array}
	 * @return {@code true} if the byte array is equal to the array, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean isContentEqual(byte[] array) {
		return isContentEqual(array, 0, array.length);
	}

	/**
	 * Returns a byte array from {@code readPosition} to {@code writePosition}.
	 * Doesn't recycle this {@link ByteBuf}.
	 *
	 * @return byte array from {@code readPosition} to {@code writePosition}
	 */
	@Contract(pure = true)
	@NotNull
	public byte[] getArray() {
		byte[] bytes = new byte[readRemaining()];
		System.arraycopy(array, readPosition, bytes, 0, bytes.length);
		return bytes;
	}

	/**
	 * Returns a byte array from {@code readPosition} to {@code writePosition}.
	 * DOES recycle this {@code ByteBuf}.
	 *
	 * @return byte array created from this {@code ByteBuf}
	 */
	@Contract(pure = false)
	@NotNull
	public byte[] asArray() {
		byte[] bytes = getArray();
		recycle();
		return bytes;
	}

	/**
	 * Returns a {@code String} created from this {@code ByteBuf} using given charset.
	 * Does not recycle this {@code ByteBuf}.
	 *
	 * @param charset charset which is used to create {@code String} from this {@code ByteBuf}.
	 * @return {@code String} from this {@code ByteBuf} in a given charset.
	 */
	@Contract(pure = true)
	public String getString(@NotNull Charset charset) {
		return new String(array, readPosition, readRemaining(), charset);
	}

	/**
	 * Returns a {@code String} created from this {@code ByteBuf} using given charset.
	 * DOES recycle this {@code ByteBuf}.
	 *
	 * @param charset charset which is used to create string from {@code ByteBuf}
	 * @return {@code String} from this {@code ByteBuf} in a given charset.
	 */
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
