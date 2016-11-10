package io.datakernel.cube.attributes;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.List;

public abstract class SimpleAttributeResolver implements AttributeResolver {
	private final Eventloop eventloop;

	protected SimpleAttributeResolver(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Override
	public Class<?>[] getKeyTypes() {
		return new Class<?>[0];
	}

	@Override
	public Class<?>[] getAttributeTypes() {
		return new Class<?>[0];
	}

	protected abstract Object[] resolveAttributes(Object[] key);

	@Override
	public void resolveAttributes(List<Object> results, KeyFunction keyFunction, AttributesFunction attributesFunction,
	                              CompletionCallback callback) {
		for (Object result : results) {
			Object[] key = keyFunction.extractKey(result);
			Object[] attributes = resolveAttributes(key);
			attributesFunction.applyAttributes(result, attributes);
		}
		callback.postComplete(eventloop);
	}
}
