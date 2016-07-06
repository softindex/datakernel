package io.datakernel.bytebuf;

import java.nio.ByteBuffer;

public class ByteBuf {
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

	// creators
	private ByteBuf(byte[] array, int readPosition, int writePosition) {
		assert readPosition >= 0 && readPosition <= writePosition && writePosition <= array.length;
		this.array = array;
		this.readPosition = readPosition;
		this.writePosition = writePosition;
	}

	public static ByteBuf empty() {
		return wrapForWriting(new byte[0]);
	}

	public static ByteBuf wrapForWriting(byte[] bytes) {
		return new ByteBuf(bytes, 0, 0);
	}

	public static ByteBuf wrapForReading(byte[] bytes) {
		return wrapForReading(bytes, 0, bytes.length);
	}

	public static ByteBuf wrapForReading(byte[] bytes, int offset, int length) {
		return new ByteBuf(bytes, offset, offset + length);
	}

	// getters & setters
	public int getReadPosition() {
		assert !isRecycled();
		return readPosition;
	}

	public int getWritePosition() {
		assert !isRecycled();
		return writePosition;
	}

	public int getLimit() {
		return array.length;
	}

	public void setReadPosition(int pos) {
		assert !isRecycled();
		assert pos >= readPosition && pos <= writePosition;
		this.readPosition = pos;
	}

	public void setWritePosition(int pos) {
		assert !isRecycled();
		assert pos >= readPosition && pos <= array.length;
		this.writePosition = pos;
	}

	// slicing
	public ByteBuf slice() {
		return slice(readPosition, remainingToRead());
	}

	public ByteBuf slice(int length) {
		return slice(readPosition, length);
	}

	public ByteBuf slice(int offset, int length) {
		assert !isRecycled();
		if (!isRecycleNeeded()) {
			return ByteBuf.wrapForReading(array, offset, length);
		}
		refs++;
		return new ByteBufSlice(this, offset, offset + length);
	}

	// recycling
	public void recycle() {
		assert !isRecycled();
		if (refs > 0 && --refs == 0) {
			assert --refs == -1;
			ByteBufPool.recycle(this);
		}
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
		assert !isRecycled();
		writePosition = 0;
		readPosition = 0;
	}

	public boolean isRecycleNeeded() {
		return refs > 0;
	}

	// byte buffers
	public ByteBuffer toByteBufferInReadMode() {
		assert !isRecycled();
		return ByteBuffer.wrap(array, readPosition, writePosition - readPosition);
	}

	public ByteBuffer toByteBufferInWriteMode() {
		assert !isRecycled();
		return ByteBuffer.wrap(array, writePosition, array.length - writePosition);
	}

//	public ByteBuffer toByteBuffer() {
//		// assume ByteBuffer is being passed in 'read mode' pos=0; lim=wPos
//		assert !isRecycled();
//		ByteBuffer buffer = ByteBuffer.wrap(array, rPos, array.length - rPos);
//		buffer.position(wPos);
//		return buffer;
//	}
//
//	public void setByteBuffer(ByteBuffer buffer) {
//		// assume ByteBuffer is being passed in 'read mode' pos=0, lim=wPos
//		assert !isRecycled();
//		assert this.array == buffer.array();
//		assert buffer.arrayOffset() == 0;
//		setReadPosition(buffer.position());
//		setWritePosition(buffer.limit());
//	}

	// getters
	public byte[] array() {
		return array;
	}

	public int remainingToWrite() {
		assert !isRecycled();
		return array.length - writePosition;
	}

	public int remainingToRead() {
		assert !isRecycled();
		return writePosition - readPosition;
	}

	public boolean canWrite() {
		assert !isRecycled();
		return writePosition != array.length;
	}

	public boolean canRead() {
		assert !isRecycled();
		return readPosition != writePosition;
	}

	public void advance(int size) {
		assert !isRecycled();
		assert writePosition + size <= array.length;
		writePosition += size;
	}

	public void skip(int size) {
		assert !isRecycled();
		assert readPosition + size <= array.length;
		readPosition += size;
	}

	public byte get() {
		assert !isRecycled();
		assert readPosition < writePosition;
		return array[readPosition++];
	}

	public byte at(int index) {
		assert !isRecycled();
		return array[index];
	}

	public byte peek() {
		assert !isRecycled();
		return array[readPosition];
	}

	public byte peek(int offset) {
		assert !isRecycled();
		assert (readPosition + offset) < writePosition;
		return array[readPosition + offset];
	}

	public void drainTo(byte[] array, int offset, int size) {
		assert !isRecycled();
		assert size >= 0 && (offset + size) <= array.length;
		assert this.readPosition + size <= this.writePosition;
		System.arraycopy(this.array, this.readPosition, array, offset, size);
		this.readPosition += size;
	}

	public void drainTo(ByteBuf buf, int size) {
		assert !buf.isRecycled();
		drainTo(buf.array, buf.writePosition, size);
		buf.writePosition += size;
	}

	// editing
	public void set(int index, byte b) {
		assert !isRecycled();
		array[index] = b;
	}

	public void put(byte b) {
		set(writePosition, b);
		writePosition++;
	}

	public void put(ByteBuf buf) {
		put(buf.array, buf.readPosition, buf.writePosition);
		buf.readPosition = buf.writePosition;
	}

	public void put(byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	public void put(byte[] bytes, int off, int lim) {
		assert !isRecycled();
		assert writePosition + (lim - off) <= array.length;
		assert bytes.length >= lim;
		int length = lim - off;
		System.arraycopy(bytes, off, array, writePosition, length);
		writePosition += length;
	}

	// miscellaneous
	@Override
	public boolean equals(Object o) {
		assert !isRecycled();
		if (this == o) return true;
		if (o == null || !(ByteBuf.class == o.getClass() || ByteBufSlice.class == o.getClass())) return false;

		ByteBuf buf = (ByteBuf) o;

		return remainingToRead() == buf.remainingToRead() &&
				arraysEquals(this.array, this.readPosition, this.writePosition, buf.array, buf.readPosition);
	}

	private boolean arraysEquals(byte[] array, int offset, int limit, byte[] arr, int off) {
		for (int i = 0; i < limit - offset; i++) {
			if (array[offset + i] != arr[off + i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		assert !isRecycled();
		int result = 1;
		for (int i = readPosition; i < writePosition; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	@Override
	public String toString() {
		char[] chars = new char[remainingToRead() < 256 ? remainingToRead() : 256];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[readPosition + i];
			chars[i] = (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}