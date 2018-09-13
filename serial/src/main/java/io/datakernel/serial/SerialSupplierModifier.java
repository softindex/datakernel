package io.datakernel.serial;

@FunctionalInterface
public interface SerialSupplierModifier<T, R> {
	R apply(SerialSupplier<T> supplier);

	static <T> SerialSupplierModifier<T, SerialSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
