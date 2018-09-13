package io.datakernel.serial.processor;

import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerFunction;

import java.util.function.Function;

public interface WithStreamToSerial<B extends WithStreamToSerial<B, I, O>, I, O> extends
		StreamConsumer<I>, WithSerialOutput<B, O>,
		StreamProducerFunction<I, SerialSupplier<O>> {

	@Override
	default SerialSupplier<O> apply(StreamProducer<I> streamProducer) {
		streamProducer.streamTo(this);
		return getOutputSupplier(new SerialZeroBuffer<>());
	}

	default Function<StreamProducer<I>, SerialSupplier<O>> toOutput(SerialQueue<O> queue) {
		return streamProducer -> {
			streamProducer.streamTo(this);
			return getOutputSupplier(queue);
		};
	}

}
