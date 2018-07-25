package io.global.common;

import java.io.IOException;

public interface Signable {
	byte[] toBytes();

	@FunctionalInterface
	interface Parser<T> {
		T parseBytes(byte[] bytes) throws IOException;
	}
}
