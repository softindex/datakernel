package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.function.Function;

public interface WithSerialToStream<B extends WithSerialToStream<B, I, O>, I, O>
		extends WithSerialInput<B, I>, StreamProducer<O> {

	default Function<SerialSupplier<I>, StreamProducer<O>> transformer() {
		return this::withInput;
	}

	default Function<StreamConsumer<O>, SerialConsumer<I>> outputTransformer() {
		return streamConsumer -> {
			streamTo(streamConsumer);
			return newInputConsumer();
		};
	}

}
