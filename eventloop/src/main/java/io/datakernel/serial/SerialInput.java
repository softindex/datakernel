package io.datakernel.serial;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;

public interface SerialInput<T> {
	MaterializedStage<Void> setInput(SerialSupplier<T> input);

	default SerialConsumer<T> getInputConsumer() {
		return getInputConsumer(new SerialZeroBuffer<>());
	}

	default SerialConsumer<T> getInputConsumer(SerialQueue<T> queue) {
		MaterializedStage<Void> extraAcknowledge = setInput(queue.getSupplier());
		SerialConsumer<T> consumer = queue.getConsumer();
		return extraAcknowledge == Stage.complete() ?
				consumer :
				consumer.withAcknowledgement(ack -> ack.thenCompose($ -> extraAcknowledge));
	}
}
