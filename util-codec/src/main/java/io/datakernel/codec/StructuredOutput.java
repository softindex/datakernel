package io.datakernel.codec;

import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * This is an abstraction that allows you to write data in uniform way
 * in different forms with different implementations of this interface
 */
public interface StructuredOutput {
	void writeNull();

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

	<T> void writeNullable(StructuredEncoder<T> encoder, @Nullable T value);

	<T> void writeList(StructuredEncoder<T> encoder, List<T> list);

	<K, V> void writeMap(StructuredEncoder<K> keyEncoder, StructuredEncoder<V> valueEncoder, Map<K, V> map);

	<T> void writeTuple(StructuredEncoder<T> encoder, T value);

	<T> void writeObject(StructuredEncoder<T> encoder, T value);

	default void writeTuple(Runnable encoder) {
		writeTuple((o1, v1) -> encoder.run(), null);
	}

	default void writeObject(Runnable encoder) {
		writeObject((o1, v1) -> encoder.run(), null);
	}

	void writeKey(String field);

	default <T> void writeKey(String field, StructuredEncoder<? super T> encoder, T value) {
		writeKey(field);
		encoder.encode(this, value);
	}

	<T> void writeCustom(Type type, T value);
}
