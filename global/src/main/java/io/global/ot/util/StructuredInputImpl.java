package io.global.ot.util;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredInput;
import io.datakernel.exception.ParseException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class StructuredInputImpl implements StructuredInput {
	private final ByteBuf buf;

	public StructuredInputImpl(ByteBuf buf) {
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
	public <T> List<T> readListEx(Supplier<? extends StructuredDecoder<? extends T>> decoderSupplier) throws ParseException {
		int size = readInt();
		List<T> list = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			StructuredDecoder<? extends T> decoder = decoderSupplier.get();
			list.add(decoder.decode(this));
		}
		return list;
	}

	@Override
	public <T> Map<String, T> readMapEx(Function<String, ? extends StructuredDecoder<? extends T>> elementDecoderSupplier) throws ParseException {
		int size = readInt();
		Map<String, T> map = new LinkedHashMap<>();
		for (int i = 0; i < size; i++) {
			String field = readString();
			StructuredDecoder<? extends T> decoder = elementDecoderSupplier.apply(field);
			map.put(field, decoder.decode(this));
		}
		return map;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T readCustom(Type type) throws ParseException {
		throw new UnsupportedOperationException();
	}
}
