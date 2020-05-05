package io.datakernel.common.tuple;

import io.datakernel.common.exception.parse.ParseException;

@FunctionalInterface
public interface TupleParser0<R> {
	R create() throws ParseException;
}
