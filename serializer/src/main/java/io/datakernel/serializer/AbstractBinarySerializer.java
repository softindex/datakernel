package io.datakernel.serializer;

import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.serializer.util.BinaryOutput;

public abstract class AbstractBinarySerializer<T> implements BinarySerializer<T> {
	@Override
	public final int encode(byte[] array, int pos, T item) {
		BinaryOutput out = new BinaryOutput(array, pos);
		encode(out, item);
		return out.pos();
	}

	@Override
	public final T decode(byte[] array, int pos) {
		return decode(new BinaryInput(array, pos));
	}
}
