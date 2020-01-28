package io.datakernel.common.tuple;

import io.datakernel.common.parse.ParseException;

@FunctionalInterface
public interface TupleParser2<T1, T2, R> {
	R create(T1 value1, T2 value2) throws ParseException;
}
