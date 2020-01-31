package io.datakernel.common.tuple;

import io.datakernel.common.parse.ParseException;

@FunctionalInterface
public interface TupleParser3<T1, T2, T3, R> {
	R create(T1 value1, T2 value2, T3 value3) throws ParseException;
}
