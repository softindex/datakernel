package io.datakernel.serial;

@FunctionalInterface
public interface SerialSupplierFunction<T, R> {
	R apply(SerialSupplier<T> supplier);

	static <T> SerialSupplierFunction<T, SerialSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
