package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
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
		SerialSupplier<O> outputSupplier = getOutput().getSupplier();
		if (this instanceof AsyncProcess) {
			((AsyncProcess) this).start();
		}
		return outputSupplier;
	}

	@Override
	default SerialConsumer<I> apply(SerialConsumer<O> consumer) {
		getOutput().set(consumer);
		SerialConsumer<I> outputConsumer = getInput().getConsumer();
		if (this instanceof AsyncProcess) {
			((AsyncProcess) this).start();
		}
		return outputConsumer;
	}

}
