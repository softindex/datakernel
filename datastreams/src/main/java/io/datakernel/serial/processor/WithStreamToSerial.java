package io.datakernel.serial.processor;

import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierFunction;

public interface WithStreamToSerial<B, I, O> extends
		StreamConsumer<I>, WithSerialOutput<B, O>,
		StreamSupplierFunction<I, SerialSupplier<O>> {

	@Override
	default SerialSupplier<O> apply(StreamSupplier<I> streamSupplier) {
		streamSupplier.streamTo(this);
		return getOutput().getSupplier();
	}

}
