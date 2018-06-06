package io.datakernel.util;

@FunctionalInterface
public interface TupleConstructor3<T1, T2, T3, R> {
	R create(T1 value1, T2 value2, T3 value3);
}
