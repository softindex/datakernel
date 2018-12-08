package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelConsumer;

public interface WithChannelOutput<B, T> extends HasChannelOutput<T> {
	@SuppressWarnings("unchecked")
	default B withOutput(ChannelConsumer<T> output) {
		getOutput().set(output);
		return (B) this;
	}
}
