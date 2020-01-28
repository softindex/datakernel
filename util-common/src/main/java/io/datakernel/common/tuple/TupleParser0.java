package io.datakernel.common.tuple;

import io.datakernel.common.parse.ParseException;

@FunctionalInterface
public interface TupleParser0<R> {
	R create() throws ParseException;
}
