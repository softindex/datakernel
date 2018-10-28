package io.datakernel.serial;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.bytebuf.ByteBuf;

public interface ByteBufsInput extends SerialInput<ByteBuf> {
	MaterializedPromise<Void> set(ByteBufsSupplier input);

	@Override
	default MaterializedPromise<Void> set(SerialSupplier<ByteBuf> input) {
		return set(ByteBufsSupplier.of(input));
	}
}
