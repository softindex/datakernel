package io.datakernel.serial.processor;

import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.StreamProducer;

import java.util.function.Function;

public interface WithSerialToStream<B extends WithSerialToStream<B, I, O>, I, O> extends
		WithSerialInput<B, I>, StreamProducer<O>,
		Function<SerialSupplier<I>, StreamProducer<O>> {

	@Override
	default StreamProducer<O> apply(SerialSupplier<I> supplier) {
		setInput(supplier);
		return this;
	}
}
