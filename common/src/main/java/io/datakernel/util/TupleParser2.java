package io.datakernel.util;

import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface TupleParser2<T1, T2, R> {
	R create(T1 value1, T2 value2) throws ParseException;
}
