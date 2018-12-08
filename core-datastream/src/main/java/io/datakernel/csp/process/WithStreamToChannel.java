package io.datakernel.csp.process;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.WithChannelOutput;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierTransformer;

public interface WithStreamToChannel<B, I, O> extends
		StreamConsumer<I>, WithChannelOutput<B, O>,
		StreamSupplierTransformer<I, ChannelSupplier<O>> {

	@Override
	default ChannelSupplier<O> transform(StreamSupplier<I> streamSupplier) {
		streamSupplier.streamTo(this);
		return getOutput().getSupplier();
	}

}
