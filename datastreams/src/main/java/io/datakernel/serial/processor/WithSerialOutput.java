package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;

public interface WithSerialOutput<B extends WithSerialOutput<B, T>, T> {
	void setOutput(SerialConsumer<T> output);

	SerialConsumer<T> getOutput();

	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		setOutput(output);
		return (B) this;
	}

	default SerialSupplier<T> getOutputSupplier() {
		return getOutputSupplier(new SerialZeroBuffer<>());
	}

	default SerialSupplier<T> getOutputSupplier(SerialQueue<T> queue) {
		setOutput(queue.getConsumer());
		return queue.getSupplier();
	}
}
