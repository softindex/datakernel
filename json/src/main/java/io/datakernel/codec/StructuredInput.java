package io.datakernel.codec;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.ParseException;
import io.datakernel.util.Tuple2;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

	default <T> List<T> readList(StructuredDecoder<T> decoder) throws ParseException {
		List<T> list = new ArrayList<>();
		ListReader reader = listReader(true);
		while (reader.hasNext()) {
			StructuredInput in = reader.next();
			list.add(decoder.decode(in));
		}
		reader.close();
		return list;
	}

	default <T> Map<String, T> readMap(StructuredDecoder<T> decoder) throws ParseException {
		Map<String, T> map = new LinkedHashMap<>();
		MapReader reader = mapReader(true);
		while (reader.hasNext()) {
			Tuple2<String, StructuredInput> entry = reader.next();
			map.put(entry.getValue1(), decoder.decode(entry.getValue2()));
		}
		reader.close();
		return map;
	}

	interface ListReader {
		boolean hasNext() throws ParseException;

		StructuredInput next() throws ParseException;

		void close() throws ParseException;
	}

	interface MapReader {
		boolean hasNext() throws ParseException;

		Tuple2<String, StructuredInput> next() throws ParseException;

		void close() throws ParseException;
	}

	ListReader listReader(boolean selfDelimited) throws ParseException;

	MapReader mapReader(boolean selfDelimited) throws ParseException;

	<T> T readCustom(Type type) throws ParseException;
}
