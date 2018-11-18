package io.datakernel.codec.binary;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredInput;
import io.datakernel.exception.ParseException;
import io.datakernel.util.Tuple2;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
			return buf.readJavaUTF8();
		} catch (Exception e) {
			throw new ParseException(e);
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
	public <T> Map<String, T> readMap(StructuredDecoder<T> decoder) throws ParseException {
		int size = readInt();
		Map<String, T> map = new LinkedHashMap<>();
		for (int i = 0; i < size; i++) {
			map.put(readString(), decoder.decode(this));
		}
		return map;
	}

	@Override
	public ListReader listReader(boolean selfDelimited) throws ParseException {
		if (selfDelimited) {
			throw new UnsupportedOperationException();
		}
		return new ListReader() {
			@Override
			public boolean hasNext() throws ParseException {
				throw new UnsupportedOperationException();
			}

			@Override
			public StructuredInput next() throws ParseException {
				return BinaryStructuredInput.this;
			}

			@Override
			public void close() throws ParseException {
			}
		};
	}

	@Override
	public MapReader mapReader(boolean selfDelimited) throws ParseException {
		if (selfDelimited) {
			throw new UnsupportedOperationException();
		}
		return new MapReader() {
			@Override
			public boolean hasNext() throws ParseException {
				throw new UnsupportedOperationException();
			}

			@Override
			public Tuple2<String, StructuredInput> next() throws ParseException {
				return new Tuple2<>(readString(), BinaryStructuredInput.this);
			}

			@Override
			public void close() throws ParseException {
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T readCustom(Type type) throws ParseException {
		throw new UnsupportedOperationException();
	}
}
