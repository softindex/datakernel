package io.datakernel.util;

import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface TupleParser6<T1, T2, T3, T4, T5, T6, R> {
	R create(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6) throws ParseException;
}
