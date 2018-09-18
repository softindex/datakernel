package io.datakernel.serial.processor;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.ByteBufsInput;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialSupplier;

public interface WithByteBufsInput<B extends WithByteBufsInput<B> & WithSerialInput<B, ByteBuf>>
		extends ByteBufsInput, WithSerialInput<B, ByteBuf> {
	@Override
	default void setInput(SerialSupplier<ByteBuf> input) {
		setInput(ByteBufsSupplier.of(input));
	}

	@SuppressWarnings("unchecked")
	default B withInput(ByteBufsSupplier byteBufsInput) {
		setInput(byteBufsInput);
		return (B) this;
	}
}
