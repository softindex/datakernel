package io.global.common;

import io.datakernel.exception.ParseException;

public interface Signable {
	byte[] toBytes();

	@FunctionalInterface
	interface Parser<T> {
		T parseBytes(byte[] bytes) throws ParseException;
	}
}
