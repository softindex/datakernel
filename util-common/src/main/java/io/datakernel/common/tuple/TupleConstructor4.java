package io.datakernel.common.tuple;

@FunctionalInterface
public interface TupleConstructor4<T1, T2, T3, T4, R> {
	R create(T1 value1, T2 value2, T3 value3, T4 value4);
}
