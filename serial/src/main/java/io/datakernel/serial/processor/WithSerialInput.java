package io.datakernel.serial.processor;

import io.datakernel.serial.HasSerialInput;
import io.datakernel.serial.SerialSupplier;

public interface WithSerialInput<B, T> extends HasSerialInput<T> {
	@SuppressWarnings("unchecked")
	default B withInput(SerialSupplier<T> input) {
		getInput().set(input);
		return (B) this;
	}
}
