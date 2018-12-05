package io.datakernel.codec.binary;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredInput;
import io.datakernel.exception.ParseException;

import java.lang.reflect.Type;
import java.util.*;

public final class BinaryStructuredInput implements StructuredInput {
	private final ByteBuf buf;

	public BinaryStructuredInput(ByteBuf buf) {
		this.buf = buf;
	}

	@Override
	public boolean readBoolean() throws ParseException {
		try {
			return buf.readBoolean();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public byte readByte() throws ParseException {
		try {
			return buf.readByte();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public int readInt() throws ParseException {
		try {
			return buf.readVarInt();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public long readLong() throws ParseException {
		try {
			return buf.readVarLong();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public int readInt32() throws ParseException {
		try {
			return buf.readInt();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public long readLong64() throws ParseException {
		try {
			return buf.readLong();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public float readFloat() throws ParseException {
		try {
			return buf.readFloat();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public double readDouble() throws ParseException {
		try {
			return buf.readDouble();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public byte[] readBytes() throws ParseException {
		int length = buf.readVarInt();
		if (length < 0 || length > buf.readRemaining()) {
			throw new ParseException("Invalid length: " + length + ", remaining: " + buf.readRemaining() + ", buf: " + buf);
		}
		byte[] result = new byte[length];
		buf.read(result);
		return result;
	}

	@Override
	public String readString() throws ParseException {
		try {
			return buf.readString();
		} catch (Exception e) {
			throw new ParseException(e);
		}
	}

	@Override
	public void readNull() throws ParseException {
		if (readBoolean()) {
			throw new ParseException("Expected NULL value");
		}
	}

	@Nullable
	@Override
	public <T> T readNullable(StructuredDecoder<T> decoder) throws ParseException {
		return readBoolean() ? decoder.decode(this) : null;
	}

	@Override
	public <T> List<T> readList(StructuredDecoder<T> decoder) throws ParseException {
		int size = readInt();
		List<T> list = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			list.add(decoder.decode(this));
		}
		return list;
	}

	@Override
	public <K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws ParseException {
		int size = readInt();
		Map<K, V> map = new LinkedHashMap<>();
		for (int i = 0; i < size; i++) {
			map.put(keyDecoder.decode(this), valueDecoder.decode(this));
		}
		return map;
	}

	@Override
	public <T> T readTuple(StructuredDecoder<T> decoder) throws ParseException {
		return decoder.decode(this);
	}

	@Override
	public <T> T readObject(StructuredDecoder<T> decoder) throws ParseException {
		return decoder.decode(this);
	}

	@Override
	public boolean hasNext() throws ParseException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readKey() throws ParseException {
		return readString();
	}

	@Override
	public <T> T readCustom(Type type) throws ParseException {
		throw new UnsupportedOperationException();
	}

	@Override
	public EnumSet<Token> getNext() throws ParseException {
		throw new UnsupportedOperationException();
	}

}
