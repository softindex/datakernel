package io.datakernel.common.tuple;

@FunctionalInterface
public interface TupleConstructor2<T1, T2, R> {
	R create(T1 value1, T2 value2);
}
