package io.datakernel.stream;

@FunctionalInterface
public interface StreamSupplierFunction<T, R> {
	R apply(StreamSupplier<T> supplier);

	static <T> StreamSupplierFunction<T, StreamSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
