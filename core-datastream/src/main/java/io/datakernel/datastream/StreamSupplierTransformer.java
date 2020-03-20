package io.datakernel.datastream;

/**
 * A transformer function that converts {@link StreamSupplier suppliers} into something else.
 * Part of the {@link StreamSupplier#transformWith} DSL.
 */
@FunctionalInterface
public interface StreamSupplierTransformer<T, R> {
	R transform(StreamSupplier<T> supplier);

	static <T> StreamSupplierTransformer<T, StreamSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
