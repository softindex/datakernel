package io.datakernel.codec;

import io.datakernel.annotation.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public interface StructuredOutput {
	void writeBoolean(boolean value);

	void writeByte(byte value);

	void writeInt(int value);

	void writeLong(long value);

	void writeInt32(int value);

	void writeLong64(long value);

	void writeFloat(float value);

	void writeDouble(double value);

	default void writeBytes(byte[] bytes) {
		writeBytes(bytes, 0, bytes.length);
	}

	void writeBytes(byte[] bytes, int off, int len);

	void writeString(String value);

	default void writeNull() {
		writeNullable((out, item) -> { throw new AssertionError();}, null);
	}

	<T> void writeNullable(StructuredEncoder<T> encoder, @Nullable T value);

	default <T> void writeList(StructuredEncoder<T> encoder, List<T> list) {
		ListWriter listWriter = listWriter(true);
		for (T item : list) {
			encoder.encode(listWriter.next(), item);
		}
		listWriter.close();
	}

	default <T> void writeMap(StructuredEncoder<T> encoder, Map<String, T> map) {
		MapWriter mapWriter = mapWriter(true);
		for (Map.Entry<String, T> entry : map.entrySet()) {
			encoder.encode(mapWriter.next(entry.getKey()), entry.getValue());
		}
		mapWriter.close();
	}

	interface ListWriter {
		StructuredOutput next();

		void close();
	}

	interface MapWriter {
		StructuredOutput next(String field);

		void close();
	}

	ListWriter listWriter(boolean selfDelimited);

	MapWriter mapWriter(boolean selfDelimited);

	<T> void writeCustom(Type type, T value);
}
