package io.datakernel.codec;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.ParseException;
import io.datakernel.util.SimpleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public interface StructuredInput {
	boolean readBoolean() throws ParseException;

	byte readByte() throws ParseException;

	int readInt() throws ParseException;

	long readLong() throws ParseException;

	int readInt32() throws ParseException;

	long readLong64() throws ParseException;

	float readFloat() throws ParseException;

	double readDouble() throws ParseException;

	byte[] readBytes() throws ParseException;

	String readString() throws ParseException;

	default Void readNull() throws ParseException {
		return readNullable(in -> { throw new ParseException();});
	}

	@Nullable
	<T> T readNullable(StructuredDecoder<T> decoder) throws ParseException;

	default <T> List<T> readList(StructuredDecoder<T> elementDecoder) throws ParseException {
		return readListEx(() -> elementDecoder);
	}

	<T> List<T> readListEx(Supplier<? extends StructuredDecoder<? extends T>> elementDecoder) throws ParseException;

	default <T> Map<String, T> readMap(StructuredDecoder<T> elementDecoder) throws ParseException {
		return this.readMapEx((String field) -> elementDecoder);
	}

	<T> Map<String, T> readMapEx(Function<String, ? extends StructuredDecoder<? extends T>> elementDecoderSupplier) throws ParseException;

	default <T> T readCustom(Class<T> type) throws ParseException {
		return readCustom(SimpleType.of(type));
	}

	<T> T readCustom(SimpleType type) throws ParseException;
}
