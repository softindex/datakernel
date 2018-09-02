package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;

public interface WithSerialOutputs<B, T> {
	void addOutput(SerialConsumer<T> output);

	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		addOutput(output);
		return (B) this;
	}

	default SerialSupplier<T> getOutputSupplier() {
		return getOutputSupplier(new SerialZeroBuffer<>());
	}

	default SerialSupplier<T> getOutputSupplier(SerialQueue<T> queue) {
		addOutput(queue.getConsumer());
		return queue.getSupplier();
	}

}
