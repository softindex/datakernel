package io.datakernel.csp.dsl;

import io.datakernel.csp.ChannelSupplier;

@FunctionalInterface
public interface ChannelSupplierTransformer<T, R> {
	R transform(ChannelSupplier<T> supplier);

	static <T> ChannelSupplierTransformer<T, ChannelSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
