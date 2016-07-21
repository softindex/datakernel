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

	private int head;
	private int tail;

	int refs;

	// creators
	private ByteBuf(byte[] array, int head, int tail) {
		assert head >= 0 && head <= tail && tail <= array.length;
		this.array = array;
		this.head = head;
		this.tail = tail;
	}

	public static ByteBuf empty() {
		return wrapForWriting(new byte[0]);
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
		return slice(head, headRemaining());
	}

	public ByteBuf slice(int length) {
		return slice(head, length);
	}

	public ByteBuf slice(int offset, int length) {
		assert !isRecycled();
		if (!isRecycleNeeded()) {
			return ByteBuf.wrap(array, offset, offset + length);
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
		tail = 0;
		head = 0;
	}

	public boolean isRecycleNeeded() {
		return refs > 0;
	}

	// byte buffers
	public ByteBuffer toHeadByteBuffer() {
		assert !isRecycled();
		return ByteBuffer.wrap(array, head, tail - head);
	}

	public ByteBuffer toTailByteBuffer() {
		assert !isRecycled();
		return ByteBuffer.wrap(array, tail, array.length - tail);
	}

	public void ofHeadByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled();
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == tail;
		this.head = byteBuffer.position();
	}

	public void ofTailByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled();
		assert array == byteBuffer.array();
		assert byteBuffer.limit() == array.length;
		this.tail = byteBuffer.position();
	}

	// getters & setters

	public byte[] array() {
		return array;
	}

	public int head() {
		assert !isRecycled();
		return head;
	}

	public int tail() {
		assert !isRecycled();
		return tail;
	}

	public int limit() {
		return array.length;
	}

	public void head(int pos) {
		assert !isRecycled();
		assert pos >= head && pos <= tail;
		this.head = pos;
	}

	public void tail(int pos) {
		assert !isRecycled();
		assert pos >= head && pos <= array.length;
		this.tail = pos;
	}

	public void moveHead(int delta) {
		assert !isRecycled();
		assert head + delta >= 0;
		assert head + delta <= tail;
		head += delta;
	}

	public void moveTail(int delta) {
		assert !isRecycled();
		assert tail + delta >= head;
		assert tail + delta <= array.length;
		tail += delta;
	}

	public int tailRemaining() {
		assert !isRecycled();
		return array.length - tail;
	}

	public int headRemaining() {
		assert !isRecycled();
		return tail - head;
	}

	public boolean canWrite() {
		assert !isRecycled();
		return tail != array.length;
	}

	public boolean canRead() {
		assert !isRecycled();
		return head != tail;
	}

	public byte get() {
		assert !isRecycled();
		assert head < tail;
		return array[head++];
	}

	public byte at(int index) {
		assert !isRecycled();
		return array[index];
	}

	public byte peek() {
		assert !isRecycled();
		return array[head];
	}

	public byte peek(int offset) {
		assert !isRecycled();
		assert (head + offset) < tail;
		return array[head + offset];
	}

	public void drainTo(byte[] array, int offset, int length) {
		assert !isRecycled();
		assert length >= 0 && (offset + length) <= array.length;
		assert this.head + length <= this.tail;
		System.arraycopy(this.array, this.head, array, offset, length);
		this.head += length;
	}

	public void drainTo(ByteBuf buf, int length) {
		assert !buf.isRecycled();
		assert buf.tail + length <= buf.array.length;
		drainTo(buf.array, buf.tail, length);
		buf.tail += length;
	}

	public void set(int index, byte b) {
		assert !isRecycled();
		array[index] = b;
	}

	public void put(byte b) {
		set(tail, b);
		tail++;
	}

	public void put(ByteBuf buf) {
		put(buf.array, buf.head, buf.tail - buf.head);
		buf.head = buf.tail;
	}

	public void put(byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	public void put(byte[] bytes, int offset, int length) {
		assert !isRecycled();
		assert tail + length <= array.length;
		assert offset + length <= bytes.length;
		System.arraycopy(bytes, offset, array, tail, length);
		tail += length;
	}

	public int find(byte b) {
		assert !isRecycled();
		for (int i = head; i < tail; i++) {
			if (array[i] == b) return i;
		}
		return -1;
	}

	public int find(byte[] bytes) {
		return find(bytes, 0, bytes.length);
	}

	public int find(byte[] bytes, int off, int len) {
		assert !isRecycled();
		L:
		for (int pos = head; pos < tail - len; pos++) {
			for (int i = 0; i < len; i++) {
				if (array[pos + i] != bytes[off + i]) {
					continue L;
				}
			}
			return pos;
		}
		return -1;
	}

	@Override
	public boolean equals(Object o) {
		assert !isRecycled();
		if (this == o) return true;
		if (o == null || !(ByteBuf.class == o.getClass() || ByteBufSlice.class == o.getClass())) return false;

		ByteBuf buf = (ByteBuf) o;

		return headRemaining() == buf.headRemaining() &&
				arraysEquals(this.array, this.head, this.tail, buf.array, buf.head);
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
		for (int i = head; i < tail; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	@Override
	public String toString() {
		char[] chars = new char[headRemaining() < 256 ? headRemaining() : 256];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[head + i];
			chars[i] = (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}