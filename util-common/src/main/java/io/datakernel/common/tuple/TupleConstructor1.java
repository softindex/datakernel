package io.datakernel.common.tuple;

@FunctionalInterface
public interface TupleConstructor1<T1, R> {
	R create(T1 value1);
}
