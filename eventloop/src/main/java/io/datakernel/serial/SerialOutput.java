package io.datakernel.serial;

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.Stage;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public interface SerialOutput<T> {
	void setOutput(SerialConsumer<T> output);

	default SerialSupplier<T> getOutputSupplier() {
		return getOutputSupplier(new SerialZeroBuffer<>());
	}

	default SerialSupplier<T> getOutputSupplier(SerialQueue<T> queue) {
		setOutput(queue.getConsumer());
		return queue.getSupplier();
	}

	default void streamTo(SerialInput<T> to) {
		streamTo(to, new SerialZeroBuffer<>());
	}

	default void streamTo(SerialInput<T> to, SerialQueue<T> queue) {
		this.setOutput(queue.getConsumer());
		to.setInput(queue.getSupplier());
		if (this instanceof AsyncProcess) {
			getCurrentEventloop().post(((AsyncProcess) this)::start);
		}
		if (to instanceof AsyncProcess) {
			getCurrentEventloop().post(((AsyncProcess) to)::start);
		}
	}

	default Stage<Void> streamTo(SerialConsumer<T> to) {
		return streamTo(to, new SerialZeroBuffer<>());
	}

	default Stage<Void> streamTo(SerialConsumer<T> to, SerialQueue<T> queue) {
		this.setOutput(queue.getConsumer());
		Stage<Void> result = queue.getSupplier().streamTo(to);
		if (this instanceof AsyncProcess) {
			((AsyncProcess) this).start();
		}
		return result;
	}

}
