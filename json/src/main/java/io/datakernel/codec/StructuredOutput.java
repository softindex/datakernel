package io.datakernel.codec;

import io.datakernel.annotation.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

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

	default <T> void writeList(StructuredEncoder<T> elementEncoder, List<T> list) {
		writeListEx(() -> elementEncoder, list);
	}

	<T> void writeListEx(Supplier<? extends StructuredEncoder<? extends T>> elementEncoderSupplier, List<T> list);

	default <T> void writeMap(StructuredEncoder<T> elementEncoder, Map<String, T> map) {
		writeMapEx($ -> elementEncoder, map);
	}

	<T> void writeMapEx(Function<String, ? extends StructuredEncoder<? extends T>> elementEncoderSupplier, Map<String, T> map);

	default <T> void writeCustom(Class<T> type, T value) {
		writeCustom((Type) type, value);
	}

	<T> void writeCustom(Type type, T value);
}
