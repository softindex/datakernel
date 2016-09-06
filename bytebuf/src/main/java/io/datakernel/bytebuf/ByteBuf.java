package io.datakernel.bytebuf;

import java.io.UnsupportedEncodingException;
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
		assert pos <= tail;
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

	public int drainTo(byte[] array, int offset, int length) {
		assert !isRecycled();
		assert length >= 0 && (offset + length) <= array.length;
		assert this.head + length <= this.tail;
		System.arraycopy(this.array, this.head, array, offset, length);
		this.head += length;
		return length;
	}

	public int drainTo(ByteBuf buf, int length) {
		assert !buf.isRecycled();
		assert buf.tail + length <= buf.array.length;
		drainTo(buf.array, buf.tail, length);
		buf.tail += length;
		return length;
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

	public byte[] getRemainingArray() {
		final byte[] bytes = new byte[headRemaining()];
		System.arraycopy(array, head, bytes, 0, bytes.length);
		return bytes;
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

	// region serialization input
	public int read(byte[] b) {
		assert !isRecycled();

		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) {
		assert !isRecycled();

		return drainTo(b, off, len);
	}

	public byte readByte() {
		assert !isRecycled();
		assert headRemaining() >= 1;

		return array[head++];
	}

	public boolean readBoolean() {
		return readByte() != 0;
	}

	public char readChar() {
		assert !isRecycled();
		assert headRemaining() >= 2;

		char c = (char) (((array[head] & 0xFF) << 8) | ((array[head + 1] & 0xFF)));
		head += 2;
		return c;
	}

	public double readDouble() {
		assert !isRecycled();

		return Double.longBitsToDouble(readLong());
	}

	public float readFloat() {
		assert !isRecycled();

		return Float.intBitsToFloat(readInt());
	}

	public int readInt() {
		assert !isRecycled();
		assert headRemaining() >= 4;

		int result = ((array[head] & 0xFF) << 24)
				| ((array[head + 1] & 0xFF) << 16)
				| ((array[head + 2] & 0xFF) << 8)
				| (array[head + 3] & 0xFF);
		head += 4;
		return result;
	}

	public int readVarInt() {
		assert !isRecycled();

		int result;
		assert headRemaining() >= 1;
		byte b = array[head];
		if (b >= 0) {
			result = b;
			head += 1;
		} else {
			assert headRemaining() >= 2;
			result = b & 0x7f;
			if ((b = array[head + 1]) >= 0) {
				result |= b << 7;
				head += 2;
			} else {
				assert headRemaining() >= 3;
				result |= (b & 0x7f) << 7;
				if ((b = array[head + 2]) >= 0) {
					result |= b << 14;
					head += 3;
				} else {
					assert headRemaining() >= 4;
					result |= (b & 0x7f) << 14;
					if ((b = array[head + 3]) >= 0) {
						result |= b << 21;
						head += 4;
					} else {
						assert headRemaining() >= 5;
						result |= (b & 0x7f) << 21;
						if ((b = array[head + 4]) >= 0) {
							result |= b << 28;
							head += 5;
						} else
							throw new IllegalArgumentException();
					}
				}
			}
		}
		return result;
	}

	public long readLong() {
		assert !isRecycled();
		assert headRemaining() >= 8;

		long result = ((long) array[head] << 56)
				| ((long) (array[head + 1] & 0xFF) << 48)
				| ((long) (array[head + 2] & 0xFF) << 40)
				| ((long) (array[head + 3] & 0xFF) << 32)
				| ((long) (array[head + 4] & 0xFF) << 24)
				| ((array[head + 5] & 0xFF) << 16)
				| ((array[head + 6] & 0xFF) << 8)
				| ((array[head + 7] & 0xFF));

		head += 8;
		return result;
	}

	public short readShort() {
		assert !isRecycled();
		assert headRemaining() >= 2;

		short result = (short) (((array[head] & 0xFF) << 8)
				| ((array[head + 1] & 0xFF)));
		head += 2;
		return result;
	}

	public String readIso88591() {
		assert !isRecycled();

		int length = readVarInt();
		return doReadIso88591(length);
	}

	public String readIso88591Nullable() {
		assert !isRecycled();

		int length = readVarInt();
		if (length == 0)
			return null;
		return doReadIso88591(length - 1);
	}

	private String doReadIso88591(int length) {
		if (length == 0)
			return "";
		if (length > headRemaining())
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
		assert !isRecycled();

		int length = readVarInt();
		return doReadCustomUTF8(length);
	}

	@Deprecated
	public String readCustomUTF8Nullable() {
		assert !isRecycled();

		int length = readVarInt();
		if (length == 0)
			return null;
		return doReadCustomUTF8(length - 1);
	}

	@Deprecated
	private String doReadCustomUTF8(int length) {
		if (length == 0)
			return "";
		if (length > headRemaining())
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
		assert !isRecycled();

		int length = readVarInt();
		return doReadUTF16(length);
	}

	public String readUTF16Nullable() {
		assert !isRecycled();

		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF16(length - 1);
	}

	private String doReadUTF16(int length) {
		if (length == 0)
			return "";
		if (length * 2 > headRemaining())
			throw new IllegalArgumentException();

		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readByte() << 8) + (readByte()));
		}
		return new String(chars, 0, length);
	}

	public long readVarLong() {
		assert !isRecycled();

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
		assert !isRecycled();

		int length = readVarInt();
		return doReadJavaUTF8(length);
	}

	public String readJavaUTF8Nullable() {
		assert !isRecycled();

		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadJavaUTF8(length - 1);
	}

	private String doReadJavaUTF8(int length) {
		if (length == 0)
			return "";
		if (length > headRemaining())
			throw new IllegalArgumentException();
		head += length;

		try {
			return new String(array, head - length, length, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}
	// endregion

	// region serialization output
	public void write(byte[] b) {
		tail = SerializationUtils.write(array, tail, b);
	}

	public void write(byte[] b, int off, int len) {
		tail = SerializationUtils.write(array, tail, b, off, len);
	}

	public void writeBoolean(boolean v) {
		tail = SerializationUtils.writeBoolean(array, tail, v);
	}

	public void writeByte(byte v) {
		tail = SerializationUtils.writeByte(array, tail, v);
	}

	public void writeChar(char v) {
		tail = SerializationUtils.writeChar(array, tail, v);
	}

	public void writeDouble(double v) {
		tail = SerializationUtils.writeDouble(array, tail, v);
	}

	public void writeFloat(float v) {
		tail = SerializationUtils.writeFloat(array, tail, v);
	}

	public void writeInt(int v) {
		tail = SerializationUtils.writeInt(array, tail, v);
	}

	public void writeLong(long v) {
		tail = SerializationUtils.writeLong(array, tail, v);
	}

	public void writeShort(short v) {
		tail = SerializationUtils.writeShort(array, tail, v);
	}

	public void writeVarInt(int v) {
		tail = SerializationUtils.writeVarInt(array, tail, v);
	}

	public void writeVarLong(long v) {
		tail = SerializationUtils.writeVarLong(array, tail, v);
	}

	public void writeIso88591(String s) {
		tail = SerializationUtils.writeIso88591(array, tail, s);
	}

	public void writeIso88591Nullable(String s) {
		tail = SerializationUtils.writeIso88591Nullable(array, tail, s);
	}

	public void writeJavaUTF8(String s) {
		tail = SerializationUtils.writeJavaUTF8(array, tail, s);
	}

	public void writeJavaUTF8Nullable(String s) {
		tail = SerializationUtils.writeJavaUTF8Nullable(array, tail, s);
	}

	@Deprecated
	public void writeCustomUTF8(String s) {
		tail = SerializationUtils.writeCustomUTF8(array, tail, s);
	}

	@Deprecated
	public void writeCustomUTF8Nullable(String s) {
		tail = SerializationUtils.writeCustomUTF8Nullable(array, tail, s);
	}

	public final void writeUTF16(String s) {
		tail = SerializationUtils.writeUTF16(array, tail, s);
	}

	public final void writeUTF16Nullable(String s) {
		tail = SerializationUtils.writeUTF16Nullable(array, tail, s);
	}
	// endregion
}