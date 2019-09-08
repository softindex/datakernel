package io.datakernel.aggregation;

import io.datakernel.promise.Promise;

public interface IdGenerator<K> {
	Promise<K> createId();
}
