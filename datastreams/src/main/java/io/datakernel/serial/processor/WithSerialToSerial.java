package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialSupplier;

import java.util.function.Function;

public interface WithSerialToSerial<B extends WithSerialToSerial<B, I, O>, I, O>
		extends WithSerialInput<B, I>, WithSerialOutput<B, O> {

	default Function<SerialSupplier<I>, SerialSupplier<O>> transformer(SerialQueue<O> queue) {
		return input -> {
			setInput(input);
			SerialSupplier<O> outputSupplier = newOutputSupplier(queue);
			if (this instanceof AsyncProcess) {
				((AsyncProcess) this).process();
			}
			return outputSupplier;
		};
	}

	default Function<SerialConsumer<O>, SerialConsumer<I>> outputTransformer(SerialQueue<I> queue) {
		return output -> {
			setOutput(output);
			SerialConsumer<I> inputConsumer = newInputConsumer(queue);
			if (this instanceof AsyncProcess) {
				((AsyncProcess) this).process();
			}
			return inputConsumer;
		};
	}

}
