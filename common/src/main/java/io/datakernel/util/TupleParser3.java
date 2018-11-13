package io.datakernel.util;

import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface TupleParser3<T1, T2, T3, R> {
	R create(T1 value1, T2 value2, T3 value3) throws ParseException;
}
