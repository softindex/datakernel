package io.datakernel.crdt.primitives;

import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;

public class LWWObject<T> implements CrdtType<LWWObject<T>> {
	private final long timestamp;
	private final T object;

	public LWWObject(long timestamp, T object) {
		this.timestamp = timestamp;
		this.object = object;
	}

	public T getObject() {
		return object;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public LWWObject<T> merge(LWWObject<T> other) {
		if (timestamp < other.timestamp) {
			return other;
		}
		return this;
	}

	@Override
	@Nullable
	public LWWObject<T> extract(long timestamp) {
		if (timestamp >= this.timestamp) {
			return null;
		}
		return this;
	}

	public static class Serializer<T> implements BinarySerializer<LWWObject<T>> {
		private final BinarySerializer<T> valueSerializer;

		public Serializer(BinarySerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, LWWObject<T> item) {
			out.writeLong(item.timestamp);
			valueSerializer.encode(out, item.object);
		}

		@Override
		public LWWObject<T> decode(BinaryInput in) {
			return new LWWObject<>(in.readLong(), valueSerializer.decode(in));
		}
	}
}
