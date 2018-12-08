package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelOutput;

public interface WithChannelOutputs<B, T> {
	ChannelOutput<T> addOutput();
}
