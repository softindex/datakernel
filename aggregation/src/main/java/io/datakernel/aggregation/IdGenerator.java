package io.datakernel.aggregation;

import io.datakernel.async.Stage;

public interface IdGenerator<K> {
	Stage<K> createId();
}
