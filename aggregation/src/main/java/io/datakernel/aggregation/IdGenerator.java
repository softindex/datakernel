package io.datakernel.aggregation;

import io.datakernel.async.ResultCallback;

public interface IdGenerator<K> {
	void createId(ResultCallback<K> callback);
}
