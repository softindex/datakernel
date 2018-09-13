package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
import io.datakernel.serial.*;

public interface WithSerialToSerial<B extends WithSerialToSerial<B, I, O>, I, O> extends
		WithSerialInput<B, I>, WithSerialOutput<B, O>,
		SerialSupplierModifier<I, SerialSupplier<O>>, SerialConsumerModifier<O, SerialConsumer<I>> {
	@Override
	default SerialSupplier<O> apply(SerialSupplier<I> supplier) {
		return serialSupplierModifier(new SerialZeroBuffer<>()).apply(supplier);
	}

	@Override
	default SerialConsumer<I> apply(SerialConsumer<O> consumer) {
		return serialConsumerModifier(new SerialZeroBuffer<>()).apply(consumer);
	}

	default SerialSupplierModifier<I, SerialSupplier<O>> serialSupplierModifier(SerialQueue<O> queue) {
		return input -> {
			setInput(input);
			SerialSupplier<O> outputSupplier = getOutputSupplier(queue);
			if (this instanceof AsyncProcess) {
				((AsyncProcess) this).process();
			}
			return outputSupplier;
		};
	}

	default SerialConsumerModifier<O, SerialConsumer<I>> serialConsumerModifier(SerialQueue<I> queue) {
		return output -> {
			setOutput(output);
			SerialConsumer<I> outputSupplier = getInputConsumer(queue);
			if (this instanceof AsyncProcess) {
				((AsyncProcess) this).process();
			}
			return outputSupplier;
		};
	}
}
