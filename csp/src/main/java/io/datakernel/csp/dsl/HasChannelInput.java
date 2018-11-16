package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelInput;

public interface HasChannelInput<T> {
	ChannelInput<T> getInput();
}
