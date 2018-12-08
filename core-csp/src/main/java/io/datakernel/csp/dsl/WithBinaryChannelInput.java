package io.datakernel.csp.dsl;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.binary.BinaryChannelSupplier;

public interface WithBinaryChannelInput<B> extends WithChannelInput<B, ByteBuf>, HasBinaryChannelInput {
	@SuppressWarnings("unchecked")
	default B withInput(BinaryChannelSupplier byteBufsInput) {
		getInput().set(byteBufsInput);
		return (B) this;
	}
}
