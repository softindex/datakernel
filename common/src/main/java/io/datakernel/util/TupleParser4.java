package io.datakernel.util;

import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface TupleParser4<T1, T2, T3, T4, R> {
	R create(T1 value1, T2 value2, T3 value3, T4 value4) throws ParseException;
}
