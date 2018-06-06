package io.datakernel.util;

@FunctionalInterface
public interface TupleConstructor6<T1, T2, T3, T4, T5, T6, R> {
	R create(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6);
}
