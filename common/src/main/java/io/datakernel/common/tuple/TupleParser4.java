package io.datakernel.common.tuple;

import io.datakernel.common.parse.ParseException;

@FunctionalInterface
public interface TupleParser4<T1, T2, T3, T4, R> {
	R create(T1 value1, T2 value2, T3 value3, T4 value4) throws ParseException;
}
