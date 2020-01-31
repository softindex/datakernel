package io.datakernel.ot.util;

import io.datakernel.promise.Promise;

public interface IdGenerator<K> {
	Promise<K> createId();
}
