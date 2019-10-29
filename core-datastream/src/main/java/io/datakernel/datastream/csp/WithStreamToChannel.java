package io.datakernel.datastream.csp;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.WithChannelOutput;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.StreamSupplierTransformer;

public interface WithStreamToChannel<B, I, O> extends
		StreamConsumer<I>, WithChannelOutput<B, O>,
		StreamSupplierTransformer<I, ChannelSupplier<O>> {

	@Override
	default ChannelSupplier<O> transform(StreamSupplier<I> streamSupplier) {
		streamSupplier.streamTo(this);
		return getOutput().getSupplier();
	}

}
