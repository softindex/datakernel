package io.datakernel.aggregation;

import io.datakernel.async.Promise;

public interface IdGenerator<K> {
	Promise<K> createId();
}
