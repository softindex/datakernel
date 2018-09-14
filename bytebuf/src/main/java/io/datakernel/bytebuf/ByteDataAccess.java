package io.datakernel.bytebuf;

public interface ByteDataAccess {
	boolean hasRemainingBytes(int remaining);

	int skip(int bytes);

	default byte readByte() {
		byte b = peekByte();
		int skip = skip(1);
		assert skip == 1;
		return b;
	}

	default boolean readBoolean() {
		return readByte() != 0;
	}

	default char readChar() {
		assert hasRemainingBytes(2);
		return (char) (((readByte() & 0xFF) << 8) | ((readByte() & 0xFF)));
	}

	default short readShort() {
		assert hasRemainingBytes(2);
		return (short) (readByte() << 8 | (readByte() & 0xFF));
	}

	default int readInt() {
		assert hasRemainingBytes(4);
		return (readByte() << 24
				| (readByte() & 0xFF) << 16
				| (readByte() & 0xFF) << 8
				| (readByte() & 0xFF));
	}

	default long readLong() {
		assert hasRemainingBytes(8);
		return ((long) readByte() << 56)
				| ((long) (readByte() & 0xFF) << 48)
				| ((long) (readByte() & 0xFF) << 40)
				| ((long) (readByte() & 0xFF) << 32)
				| ((long) (readByte() & 0xFF) << 24)
				| ((readByte() & 0xFF) << 16)
				| ((readByte() & 0xFF) << 8)
				| ((readByte() & 0xFF));
	}

	default float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	default double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	default int readVarInt() {
		assert hasRemainingBytes(1);
		int result;
		byte b = readByte();
		if (b >= 0) {
			return b;
		}
		assert hasRemainingBytes(2);
		result = b & 0x7f;
		if ((b = readByte()) >= 0) {
			return result | b << 7;
		}
		assert hasRemainingBytes(3);
		result |= (b & 0x7f) << 7;
		if ((b = readByte()) >= 0) {
			return result | b << 14;
		}
		assert hasRemainingBytes(4);
		result |= (b & 0x7f) << 14;
		if ((b = readByte()) >= 0) {
			return result | b << 21;
		}
		assert hasRemainingBytes(5);
		result |= (b & 0x7f) << 21;
		if ((b = readByte()) >= 0) {
			return result | b << 28;
		}
		throw new IllegalArgumentException();
	}

	byte peekByte(int offset);

	default byte peekByte() {
		return peekByte(0);
	}

	default boolean peekBoolean(int offset) {
		return peekByte(offset) != 0;
	}

	default boolean peekBoolean() {
		return peekBoolean(0);
	}

	default char peekChar(int offset) {
		assert hasRemainingBytes(offset + 2);
		return (char) (((peekByte(offset) & 0xFF) << 8) | ((peekByte(offset + 1) & 0xFF)));
	}

	default char peekChar() {
		return peekChar(0);
	}

	default short peekShort(int offset) {
		assert hasRemainingBytes(offset + 2);
		return (short) (peekByte(offset) << 8 | (peekByte(offset + 1) & 0xFF));
	}

	default short peekShort() {
		return peekShort(0);
	}

	default int peekInt(int offset) {
		assert hasRemainingBytes(offset + 4);
		return (peekByte(offset) << 24
				| (peekByte(offset + 1) & 0xFF) << 16
				| (peekByte(offset + 2) & 0xFF) << 8
				| (peekByte(offset + 3) & 0xFF));
	}

	default int peekInt() {
		return peekInt(0);
	}

	default long peekLong(int offset) {
		assert hasRemainingBytes(offset + 8);
		return ((long) peekByte(offset) << 56)
				| ((long) (peekByte(offset + 1) & 0xFF) << 48)
				| ((long) (peekByte(offset + 2) & 0xFF) << 40)
				| ((long) (peekByte(offset + 3) & 0xFF) << 32)
				| ((long) (peekByte(offset + 4) & 0xFF) << 24)
				| ((peekByte(offset + 5) & 0xFF) << 16)
				| ((peekByte(offset + 6) & 0xFF) << 8)
				| ((peekByte(offset + 7) & 0xFF));
	}

	default long peekLong() {
		return peekLong(0);
	}

	default float peekFloat(int offset) {
		return Float.intBitsToFloat(peekInt(offset));
	}

	default float peekFloat() {
		return peekFloat(0);
	}

	default double peekDouble(int offset) {
		return Double.longBitsToDouble(peekLong(offset));
	}

	default double peekDouble() {
		return peekDouble(0);
	}

	default int peekVarInt(int offset) {
		assert hasRemainingBytes(offset + 1);
		int result;
		byte b = peekByte(offset);
		if (b >= 0) {
			return b;
		}
		assert hasRemainingBytes(offset + 2);
		result = b & 0x7f;
		if ((b = peekByte(offset + 1)) >= 0) {
			return result | b << 7;
		}
		assert hasRemainingBytes(offset + 3);
		result |= (b & 0x7f) << 7;
		if ((b = peekByte(offset + 2)) >= 0) {
			return result | b << 14;
		}
		assert hasRemainingBytes(offset + 4);
		result |= (b & 0x7f) << 14;
		if ((b = peekByte(offset + 3)) >= 0) {
			return result | b << 21;
		}
		assert hasRemainingBytes(offset + 5);
		result |= (b & 0x7f) << 21;
		if ((b = peekByte(offset + 4)) >= 0) {
			return result | b << 28;
		}
		throw new IllegalArgumentException();
	}
}
