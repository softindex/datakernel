package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelSupplier;

public interface WithChannelInput<B, T> extends HasChannelInput<T> {
	@SuppressWarnings("unchecked")
	default B withInput(ChannelSupplier<T> input) {
		getInput().set(input);
		return (B) this;
	}
}
