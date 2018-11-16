package io.global.ot.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.StructuredOutput;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class StructuredOutputImpl implements StructuredOutput {
	private ByteBuf buf = ByteBufPool.allocate(256);

	public ByteBuf getBuf() {
		return buf;
	}

	@Override
	public void writeBoolean(boolean value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 1);
		buf.writeBoolean(value);
	}

	@Override
	public void writeByte(byte value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 1);
		buf.writeByte(value);
	}

	@Override
	public void writeInt(int value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 5);
		buf.writeVarInt(value);
	}

	@Override
	public void writeLong(long value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 9);
		buf.writeVarLong(value);
	}

	@Override
	public void writeInt32(int value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 4);
		buf.writeInt(value);
	}

	@Override
	public void writeLong64(long value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 8);
		buf.writeLong(value);
	}

	@Override
	public void writeFloat(float value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 4);
		buf.writeFloat(value);
	}

	@Override
	public void writeDouble(double value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 8);
		buf.writeDouble(value);
	}

	@Override
	public void writeBytes(byte[] bytes, int off, int len) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 5 + len);
		buf.writeVarInt(bytes.length);
		buf.write(bytes, off, len);
	}

	@Override
	public void writeString(String value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 5 + value.length() * 5);
		buf.writeJavaUTF8(value);
	}

	@Override
	public <T> void writeNullable(StructuredEncoder<T> encoder, T value) {
		if (value != null) {
			writeBoolean(true);
			encoder.encode(this, value);
		} else {
			writeBoolean(false);
		}
	}

	@Override
	public <T> void writeListEx(Supplier<? extends StructuredEncoder<? extends T>> elementEncoderSupplier, List<T> list) {
		writeInt(list.size());
		for (T item : list) {
			StructuredEncoder<T> encoder = (StructuredEncoder<T>) elementEncoderSupplier.get();
			encoder.encode(this, item);
		}
	}

	@Override
	public <T> void writeMapEx(Function<String, ? extends StructuredEncoder<? extends T>> elementEncoderSupplier, Map<String, T> map) {
		writeInt(map.size());
		for (Map.Entry<String, T> entry : map.entrySet()) {
			String field = entry.getKey();
			writeString(field);
			StructuredEncoder<T> encoder = (StructuredEncoder<T>) elementEncoderSupplier.apply(field);
			encoder.encode(this, entry.getValue());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void writeCustom(Type type, T value) {
		throw new UnsupportedOperationException();
	}
}
