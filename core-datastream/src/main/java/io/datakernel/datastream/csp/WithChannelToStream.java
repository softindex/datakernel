package io.datakernel.datastream.csp;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.ChannelSupplierTransformer;
import io.datakernel.csp.dsl.WithChannelInput;
import io.datakernel.datastream.StreamSupplier;

/**
 * This interface is a shortcut for implementing transformers that convert
 * {@link ChannelSupplier channel suppliers} to {@link StreamSupplier stream suppliers}
 * and are useful through both DSLs.
 */
public interface WithChannelToStream<Self, I, O> extends
		StreamSupplier<O>,
		WithChannelInput<Self, I>,
		ChannelSupplierTransformer<I, StreamSupplier<O>> {

	@Override
	default StreamSupplier<O> transform(ChannelSupplier<I> supplier) {
		getInput().set(supplier);
		return this;
	}
}
