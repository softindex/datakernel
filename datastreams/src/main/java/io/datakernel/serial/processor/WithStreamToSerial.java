package io.datakernel.serial.processor;

import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierFunction;

import java.util.function.Function;

public interface WithStreamToSerial<B extends WithStreamToSerial<B, I, O>, I, O> extends
		StreamConsumer<I>, WithSerialOutput<B, O>,
		StreamSupplierFunction<I, SerialSupplier<O>> {

	@Override
	default SerialSupplier<O> apply(StreamSupplier<I> streamSupplier) {
		streamSupplier.streamTo(this);
		return getOutputSupplier(new SerialZeroBuffer<>());
	}

	default Function<StreamSupplier<I>, SerialSupplier<O>> toOutput(SerialQueue<O> queue) {
		return streamSupplier -> {
			streamSupplier.streamTo(this);
			return getOutputSupplier(queue);
		};
	}

}
