package io.datakernel.serial.processor;

import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierFunction;
import io.datakernel.stream.StreamSupplier;

public interface WithSerialToStream<B extends WithSerialToStream<B, I, O>, I, O> extends
		WithSerialInput<B, I>, StreamSupplier<O>,
		SerialSupplierFunction<I, StreamSupplier<O>> {

	@Override
	default StreamSupplier<O> apply(SerialSupplier<I> supplier) {
		setInput(supplier);
		return this;
	}
}
