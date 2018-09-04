package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;

public interface WithSerialInput<B extends WithSerialInput<B, T>, T> {
	void setInput(SerialSupplier<T> input);

	SerialSupplier<T> getInput();

	@SuppressWarnings("unchecked")
	default B withInput(SerialSupplier<T> input) {
		setInput(input);
		return (B) this;
	}

	default SerialConsumer<T> newInputConsumer() {
		return newInputConsumer(new SerialZeroBuffer<>());
	}

	default SerialConsumer<T> newInputConsumer(SerialQueue<T> queue) {
		setInput(queue.getSupplier());
		return queue.getConsumer();
	}
}
