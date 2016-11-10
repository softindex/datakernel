package io.datakernel.cube.attributes;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;

import java.util.List;

public abstract class AbstractAttributeResolver<K, A> implements AttributeResolver {
	@Override
	public abstract Class<?>[] getKeyTypes();

	protected abstract K toKey(Object[] keyArray);

	@Override
	public abstract Class<?>[] getAttributeTypes();

	protected abstract Object[] toAttributes(A attributes);

	protected abstract A resolveAttributes(K key);

	protected void prepareToResolveAttributes(List<Object> results, KeyFunction keyFunction, AttributesFunction attributesFunction,
	                                          CompletionCallback callback) {
		doResolveAttributes(results, keyFunction, attributesFunction, callback);
	}

	private void doResolveAttributes(List<Object> results, KeyFunction keyFunction, AttributesFunction attributesFunction, CompletionCallback callback) {
		for (Object result : results) {
			K key = toKey(keyFunction.extractKey(result));
			A attributes = resolveAttributes(key);
			if (attributes != null) {
				attributesFunction.applyAttributes(result, toAttributes(attributes));
			}
		}
		callback.postComplete();
	}

	@Override
	public final void resolveAttributes(final List<Object> results, final KeyFunction keyFunction, final AttributesFunction attributesFunction,
	                                    final CompletionCallback callback) {
		prepareToResolveAttributes(results, keyFunction, attributesFunction, new ForwardingCompletionCallback(callback) {
			@Override
			protected void onComplete() {
				doResolveAttributes(results, keyFunction, attributesFunction, callback);
			}
		});
	}

}
