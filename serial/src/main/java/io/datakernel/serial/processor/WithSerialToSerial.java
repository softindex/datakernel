package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerFunction;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierFunction;

public interface WithSerialToSerial<B, I, O> extends
		WithSerialInput<B, I>,
		WithSerialOutput<B, O>,
		SerialSupplierFunction<I, SerialSupplier<O>>,
		SerialConsumerFunction<O, SerialConsumer<I>> {

	@Override
	default SerialSupplier<O> apply(SerialSupplier<I> supplier) {
		getInput().set(supplier);
		return getOutput().getSupplier();
	}

	@Override
	default SerialConsumer<I> apply(SerialConsumer<O> consumer) {
		getOutput().set(consumer);
		return getInput().getConsumer();
	}

}
