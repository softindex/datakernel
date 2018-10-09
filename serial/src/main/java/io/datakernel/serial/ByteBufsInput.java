package io.datakernel.serial;

import io.datakernel.async.MaterializedStage;
import io.datakernel.bytebuf.ByteBuf;

public interface ByteBufsInput extends SerialInput<ByteBuf> {
	MaterializedStage<Void> set(ByteBufsSupplier input);

	@Override
	default MaterializedStage<Void> set(SerialSupplier<ByteBuf> input) {
		return set(ByteBufsSupplier.of(input));
	}
}
