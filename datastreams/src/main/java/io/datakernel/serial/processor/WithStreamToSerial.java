package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.function.Function;

public interface WithStreamToSerial<B extends WithStreamToSerial<B, I, O>, I, O>
		extends StreamConsumer<I>, WithSerialOutput<B, O> {

	default Function<StreamProducer<I>, SerialSupplier<O>> transformer(SerialQueue<O> queue) {
		return streamProducer -> {
			streamProducer.streamTo(this);
			return newOutputSupplier(queue);
		};
	}

	default Function<SerialConsumer<O>, StreamConsumer<I>> outputTransformer() {
		return this::withOutput;
	}

}
