package io.datakernel.util;

import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface TupleParser0<R> {
	R create() throws ParseException;
}
