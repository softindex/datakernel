package io.datakernel.codec.binary;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.StructuredOutput;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public final class BinaryStructuredOutput implements StructuredOutput {
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
	public void writeNull() {
		writeBoolean(false);
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
	public <T> void writeList(StructuredEncoder<T> encoder, List<T> list) {
		writeInt(list.size());
		for (T item : list) {
			encoder.encode(this, item);
		}
	}

	@Override
	public <K, V> void writeMap(StructuredEncoder<K> keyEncoder, StructuredEncoder<V> valueEncoder, Map<K, V> map) {
		writeInt(map.size());
		for (Map.Entry<K, V> entry : map.entrySet()) {
			keyEncoder.encode(this, entry.getKey());
			valueEncoder.encode(this, entry.getValue());
		}
	}

	@Override
	public <T> void writeTuple(StructuredEncoder<T> encoder, T value) {
		encoder.encode(this, value);
	}

	@Override
	public <T> void writeObject(StructuredEncoder<T> encoder, T value) {
		encoder.encode(this, value);
	}

	@Override
	public void writeKey(String field) {
		writeString(field);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void writeCustom(Type type, T value) {
		throw new UnsupportedOperationException();
	}
}
