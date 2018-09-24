package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
import io.datakernel.serial.*;

public interface WithSerialToSerial<B extends WithSerialToSerial<B, I, O>, I, O> extends
		WithSerialInput<B, I>, WithSerialOutput<B, O>,
		SerialSupplierFunction<I, SerialSupplier<O>>, SerialConsumerFunction<O, SerialConsumer<I>> {
	@Override
	default SerialSupplier<O> apply(SerialSupplier<I> supplier) {
		this.setInput(supplier);
		SerialSupplier<O> outputSupplier = getOutputSupplier(new SerialZeroBuffer<>());
		if (this instanceof AsyncProcess) {
			((AsyncProcess) this).start();
		}
		return outputSupplier;
	}

	@Override
	default SerialConsumer<I> apply(SerialConsumer<O> consumer) {
		this.setOutput(consumer);
		SerialConsumer<I> outputConsumer = getInputConsumer(new SerialZeroBuffer<>());
		if (this instanceof AsyncProcess) {
			((AsyncProcess) this).start();
		}
		return outputConsumer;
	}

}
