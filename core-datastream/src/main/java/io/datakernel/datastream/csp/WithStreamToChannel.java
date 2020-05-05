package io.datakernel.datastream.csp;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.WithChannelOutput;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamSupplierTransformer;

/**
 * This interface is a shortcut for implementing transformers that convert
 * {@link StreamSupplier stream suppliers} to {@link ChannelSupplier channel suppliers}
 * and are useful through both DSLs.
 */
public interface WithStreamToChannel<Self, I, O> extends
		StreamConsumer<I>,
		WithChannelOutput<Self, O>,
		StreamSupplierTransformer<I, ChannelSupplier<O>> {

	@Override
	default ChannelSupplier<O> transform(StreamSupplier<I> streamSupplier) {
		streamSupplier.streamTo(this);
		return getOutput().getSupplier();
	}
}
