package io.datakernel.codec;

import io.datakernel.common.exception.parse.ParseException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * This is an abstraction that allows you to read data in uniform way
 * from different sources with different implementations of this interface
 */
public interface StructuredInput {
	void readNull() throws ParseException;

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

	@Nullable <T> T readNullable(StructuredDecoder<T> decoder) throws ParseException;

	boolean hasNext() throws ParseException;

	String readKey() throws ParseException;

	default void readKey(String expectedName) throws ParseException {
		String actualName = readKey();
		if (!expectedName.equals(actualName)) {
			throw new ParseException("Expected field: " + expectedName + ", but was: " + actualName);
		}
	}

	default <T> T readKey(String expectedName, StructuredDecoder<T> decoder) throws ParseException {
		readKey(expectedName);
		return decoder.decode(this);
	}

	<T> List<T> readList(StructuredDecoder<T> decoder) throws ParseException;

	<K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws ParseException;

	<T> T readTuple(StructuredDecoder<T> decoder) throws ParseException;

	<T> T readObject(StructuredDecoder<T> decoder) throws ParseException;

	@FunctionalInterface
	interface ParserRunnable {
		void run() throws ParseException;
	}

	default void readTuple(ParserRunnable decoder) throws ParseException {
		readTuple(in -> {
			decoder.run();
			return null;
		});
	}

	default void readObject(ParserRunnable decoder) throws ParseException {
		readObject(in -> {
			decoder.run();
			return null;
		});
	}

	<T> T readCustom(Type type) throws ParseException;

	enum Token {
		NULL, BOOLEAN, BYTE, INT, LONG, FLOAT, DOUBLE, STRING, BYTES, LIST, MAP, TUPLE, OBJECT
	}

	EnumSet<Token> getNext() throws ParseException;
}
