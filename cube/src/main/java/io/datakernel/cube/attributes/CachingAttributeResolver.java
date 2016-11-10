package io.datakernel.cube.attributes;

import io.datakernel.async.CompletionCallback;
import io.datakernel.util.Function;

import java.util.List;
import java.util.Map;

public abstract class CachingAttributeResolver<K, A> extends AbstractAttributeResolver<K, A> {
	protected Map<K, A> cache;

	@Override
	protected final A resolveAttributes(K key) {
		return cache.get(key);
	}

	protected abstract void fillCache(List<Object> results, Function<Object, K> keyFunction,
	                                  CompletionCallback callback);

	protected abstract void updateCache(List<Object> results, Function<Object, K> keyFunction,
	                                    CompletionCallback callback);

	@Override
	protected final void prepareToResolveAttributes(List<Object> results, final KeyFunction keyFunction, AttributesFunction attributesFunction, CompletionCallback callback) {
		Function<Object, K> internalKeyFunction = new Function<Object, K>() {
			@Override
			public K apply(Object result) {
				return toKey(keyFunction.extractKey(result));
			}
		};
		if (cache == null) {
			fillCache(results, internalKeyFunction, callback);
		} else {
			updateCache(results, internalKeyFunction, callback);
		}
	}

}
