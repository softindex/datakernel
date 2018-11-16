package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelConsumer;

@FunctionalInterface
public interface ChannelConsumerTransformer<T, R> {
	R transform(ChannelConsumer<T> consumer);

	static <T> ChannelConsumerTransformer<T, ChannelConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
