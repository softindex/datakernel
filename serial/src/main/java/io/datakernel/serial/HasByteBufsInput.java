package io.datakernel.serial;

import io.datakernel.bytebuf.ByteBuf;

public interface HasByteBufsInput extends HasSerialInput<ByteBuf> {
	@Override
	ByteBufsInput getInput();
}
