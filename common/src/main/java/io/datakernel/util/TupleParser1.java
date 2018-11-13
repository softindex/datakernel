package io.datakernel.util;

import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface TupleParser1<T1, R> {
	R create(T1 value1) throws ParseException;
}
