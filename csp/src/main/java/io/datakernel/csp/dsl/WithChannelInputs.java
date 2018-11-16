package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelInput;

public interface WithChannelInputs<B, T> {
	ChannelInput<T> addInput();
}
