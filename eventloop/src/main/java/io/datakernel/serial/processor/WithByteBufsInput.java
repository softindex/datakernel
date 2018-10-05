package io.datakernel.serial.processor;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.HasByteBufsInput;

public interface WithByteBufsInput<B> extends WithSerialInput<B, ByteBuf>, HasByteBufsInput {
	@SuppressWarnings("unchecked")
	default B withInput(ByteBufsSupplier byteBufsInput) {
		getByteBufsInput().setInput(byteBufsInput);
		return (B) this;
	}
}
