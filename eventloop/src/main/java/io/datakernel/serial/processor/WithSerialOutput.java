package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialOutput;

public interface WithSerialOutput<B extends WithSerialOutput<B, T>, T> extends SerialOutput<T> {
	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		setOutput(output);
		return (B) this;
	}
}
