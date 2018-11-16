package io.datakernel.csp.dsl;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.binary.BinaryChannelInput;

public interface HasBinaryChannelInput extends HasChannelInput<ByteBuf> {
	@Override
	BinaryChannelInput getInput();
}
