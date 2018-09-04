package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;

public interface WithSerialOutputs<B extends WithSerialOutputs<B, T>, T> {
	void addOutput(SerialConsumer<T> output);

	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		addOutput(output);
		return (B) this;
	}

	default SerialSupplier<T> newOutputSupplier() {
		return newOutputSupplier(new SerialZeroBuffer<>());
	}

	default SerialSupplier<T> newOutputSupplier(SerialQueue<T> queue) {
		addOutput(queue.getConsumer());
		return queue.getSupplier();
	}

}
