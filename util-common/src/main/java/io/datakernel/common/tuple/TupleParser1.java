package io.datakernel.common.tuple;

import io.datakernel.common.exception.parse.ParseException;

@FunctionalInterface
public interface TupleParser1<T1, R> {
	R create(T1 value1) throws ParseException;
}
