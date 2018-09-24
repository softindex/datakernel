package io.datakernel.serial;

import io.datakernel.async.MaterializedStage;

public interface ByteBufsInput {
	MaterializedStage<Void> setInput(ByteBufsSupplier input);
}
