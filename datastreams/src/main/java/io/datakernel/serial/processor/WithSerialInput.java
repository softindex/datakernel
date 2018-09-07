package io.datakernel.serial.processor;

import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialSupplier;

public interface WithSerialInput<B extends WithSerialInput<B, T>, T> extends SerialInput<T> {
	@SuppressWarnings("unchecked")
	default B withInput(SerialSupplier<T> input) {
		setInput(input);
		return (B) this;
	}
}
