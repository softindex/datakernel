package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
import io.datakernel.serial.*;

public interface WithSerialToSerial<B extends WithSerialToSerial<B, I, O>, I, O> extends
		WithSerialInput<B, I>, WithSerialOutput<B, O>,
		SerialSupplierModifier<I, O>, SerialConsumerModifier<O, I> {

	@Override
	default SerialSupplier<O> applyTo(SerialSupplier<I> supplier) {
		return serialSupplierModifier(new SerialZeroBuffer<>()).applyTo(supplier);
	}

	@Override
	default SerialConsumer<I> applyTo(SerialConsumer<O> consumer) {
		return serialConsumerModifier(new SerialZeroBuffer<>()).applyTo(consumer);
	}

	default SerialSupplierModifier<I, O> serialSupplierModifier(SerialQueue<O> queue) {
		return input -> {
			setInput(input);
			SerialSupplier<O> outputSupplier = getOutputSupplier(queue);
			if (this instanceof AsyncProcess) {
				((AsyncProcess) this).process();
			}
			return outputSupplier;
		};
	}

	default SerialConsumerModifier<O, I> serialConsumerModifier(SerialQueue<I> queue) {
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
