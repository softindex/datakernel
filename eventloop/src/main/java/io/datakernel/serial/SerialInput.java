package io.datakernel.serial;

public interface SerialInput<T> {
	void setInput(SerialSupplier<T> input);

	default SerialConsumer<T> getInputConsumer() {
		return getInputConsumer(new SerialZeroBuffer<>());
	}

	default SerialConsumer<T> getInputConsumer(SerialQueue<T> queue) {
		setInput(queue.getSupplier());
		return queue.getConsumer();
	}
}
