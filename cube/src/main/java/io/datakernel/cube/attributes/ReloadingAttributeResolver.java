package io.datakernel.cube.attributes;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.util.Function;

import java.util.List;

public abstract class ReloadingAttributeResolver<K, A, T> extends CachingAttributeResolver<K, A> {
	private T timestamp;

	protected abstract void reload(@Nullable T lastTimestamp, ResultCallback<T> callback);

	private void doReload(final CompletionCallback callback) {
		reload(timestamp, new ForwardingResultCallback<T>(callback) {
			@Override
			protected void onResult(T result) {
				ReloadingAttributeResolver.this.timestamp = result;
			}
		});
	}

	@Override
	protected final void fillCache(List<Object> results, Function<Object, K> keyFunction, CompletionCallback callback) {
		doReload(callback);
	}

	@Override
	protected final void updateCache(List<Object> results, Function<Object, K> keyFunction, CompletionCallback callback) {
		doReload(callback);
	}
}
