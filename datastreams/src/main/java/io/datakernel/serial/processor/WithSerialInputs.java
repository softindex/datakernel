package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;

public interface WithSerialInputs<B, T> {
	void addInput(SerialSupplier<T> input);

	default void withInput(SerialSupplier<T> input) {
		addInput(input);
	}

	default SerialConsumer<T> getInputConsumer() {
		return getInputConsumer(new SerialZeroBuffer<>());
	}

	default SerialConsumer<T> getInputConsumer(SerialQueue<T> queue) {
		addInput(queue.getSupplier());
		return queue.getConsumer();
	}

}
