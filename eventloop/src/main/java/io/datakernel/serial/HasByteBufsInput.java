package io.datakernel.serial;

import io.datakernel.bytebuf.ByteBuf;

public interface HasByteBufsInput extends HasSerialInput<ByteBuf> {
	ByteBufsInput getByteBufsInput();

	@Override
	default SerialInput<ByteBuf> getInput() {
		return input -> getByteBufsInput().setInput(ByteBufsSupplier.of(input));
	}
}
