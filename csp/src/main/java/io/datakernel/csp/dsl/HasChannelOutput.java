package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelOutput;

public interface HasChannelOutput<T> {
	ChannelOutput<T> getOutput();
}
