package io.datakernel.datastream.csp;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.ChannelSupplierTransformer;
import io.datakernel.csp.dsl.WithChannelInput;
import io.datakernel.datastream.StreamSupplier;

public interface WithChannelToStream<B, I, O> extends
		WithChannelInput<B, I>, StreamSupplier<O>,
		ChannelSupplierTransformer<I, StreamSupplier<O>> {

	@Override
	default StreamSupplier<O> transform(ChannelSupplier<I> supplier) {
		getInput().set(supplier);
		return this;
	}
}
