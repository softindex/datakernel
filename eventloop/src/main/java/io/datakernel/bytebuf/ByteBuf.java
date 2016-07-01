package io.datakernel.bytebuf;

import java.nio.ByteBuffer;

public class ByteBuf {
	static final class ByteBufSlice extends ByteBuf {
		private ByteBuf root;

		private ByteBufSlice(ByteBuf buf, int rPos, int wPos, int limit) {
			super(buf.array, rPos, wPos, limit);
			this.root = buf;
		}

		@Override
		public void recycle() {
			root.recycle();
		}

		@Override
		public ByteBuf slice(int offset, int limit) {
			return root.slice(offset, limit);
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

	private int rPos;
	private int wPos;

	int refs;

	// creators
	private ByteBuf(byte[] array, int rPos, int wPos, int limit) {
		assert rPos >= 0 && wPos <= limit && rPos <= wPos && limit <= array.length;
		this.array = array;
		this.rPos = rPos;
		this.wPos = wPos;
	}

	public static ByteBuf empty() {
		return new ByteBuf(new byte[0], 0, 0, 0);
	}

	public static ByteBuf create(int size) {
		return new ByteBuf(new byte[size], 0, 0, size);
	}

	public static ByteBuf wrap(byte[] bytes) {
		return wrap(bytes, 0, bytes.length);
	}

	public static ByteBuf wrap(byte[] bytes, int offset, int length) {
		int limit = offset + length;
		return new ByteBuf(bytes, offset, limit, limit);
	}

	// getters & setters
	public int getReadPosition() {
		assert !isRecycled();
		return rPos;
	}

	public int getWritePosition() {
		assert !isRecycled();
		return wPos;
	}

	public int getLimit() {
		return array.length;
	}

	public void setReadPosition(int pos) {
		assert !isRecycled();
		assert pos >= rPos && pos <= wPos;
		this.rPos = pos;
	}

	public void setWritePosition(int pos) {
		assert !isRecycled();
		assert pos >= rPos && pos <= array.length;
		this.wPos = pos;
	}

	// slicing
	public ByteBuf slice() {
		return slice(rPos, wPos);
	}

	public ByteBuf slice(int size) {
		return slice(rPos, rPos + size);
	}

	public ByteBuf slice(int offset, int limit) {
		assert !isRecycled();
		if (!isRecycleNeeded()) {
			return ByteBuf.wrap(array, offset, limit - offset);
		}
		refs++;
		return new ByteBufSlice(this, offset, limit, limit);
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
		wPos = 0;
		rPos = 0;
	}

	public boolean isRecycleNeeded() {
		return refs > 0;
	}

	// byte buffers
	public ByteBuffer toByteBufferInReadMode() {
		assert !isRecycled();
		return ByteBuffer.wrap(array, rPos, wPos - rPos);
	}

	public ByteBuffer toByteBufferInWriteMode() {
		assert !isRecycled();
		return ByteBuffer.wrap(array, wPos, array.length - wPos);
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
		return array.length - wPos;
	}

	public int remainingToRead() {
		assert !isRecycled();
		return wPos - rPos;
	}

	public boolean canWrite() {
		assert !isRecycled();
		return wPos != array.length;
	}

	public boolean canRead() {
		assert !isRecycled();
		return rPos != wPos;
	}

	public void advance(int size) {
		assert !isRecycled();
		assert wPos + size <= array.length;
		wPos += size;
	}

	public void skip(int size) {
		assert !isRecycled();
		assert rPos + size <= array.length;
		rPos += size;
	}

	public byte get() {
		assert !isRecycled();
		assert rPos < wPos;
		return array[rPos++];
	}

	public byte at(int index) {
		assert !isRecycled();
		return array[index];
	}

	public byte peek() {
		assert !isRecycled();
		return array[rPos];
	}

	public byte peek(int offset) {
		assert !isRecycled();
		assert (rPos + offset) < wPos;
		return array[rPos + offset];
	}

	public void drainTo(byte[] array, int offset, int size) {
		assert !isRecycled();
		assert size >= 0 && (offset + size) <= array.length;
		assert this.rPos + size <= this.wPos;
		System.arraycopy(this.array, this.rPos, array, offset, size);
		this.rPos += size;
	}

	public void drainTo(ByteBuf buf, int size) {
		assert !buf.isRecycled();
		drainTo(buf.array, buf.wPos, size);
		buf.wPos += size;
	}

	// editing
	public void set(int index, byte b) {
		assert !isRecycled();
		array[index] = b;
	}

	public void put(byte b) {
		set(wPos, b);
		wPos++;
	}

	public void put(ByteBuf buf) {
		put(buf.array, buf.rPos, buf.wPos);
		buf.rPos = buf.wPos;
	}

	public void put(byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	public void put(byte[] bytes, int off, int lim) {
		assert !isRecycled();
		assert wPos + (lim - off) <= array.length;
		assert bytes.length >= lim;
		int length = lim - off;
		System.arraycopy(bytes, off, array, wPos, length);
		wPos += length;
	}

	// miscellaneous
	@Override
	public boolean equals(Object o) {
		assert !isRecycled();
		if (this == o) return true;
		if (o == null || !(ByteBuf.class == o.getClass() || ByteBufSlice.class == o.getClass())) return false;

		ByteBuf buf = (ByteBuf) o;

		return remainingToRead() == buf.remainingToRead() &&
				arraysEquals(this.array, this.rPos, this.wPos, buf.array, buf.rPos);
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
		for (int i = rPos; i < wPos; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	@Override
	public String toString() {
		char[] chars = new char[remainingToRead() < 256 ? remainingToRead() : 256];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[rPos + i];
			chars[i] = (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}