/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.cube.attributes;

import io.datakernel.async.Stages;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public abstract class AbstractAttributeResolver<K, A> implements AttributeResolver {
	@Override
	public abstract Class<?>[] getKeyTypes();

	protected abstract K toKey(Object[] keyArray);

	@Override
	public abstract Map<String, Class<?>> getAttributeTypes();

	protected abstract Object[] toAttributes(A attributes);

	protected abstract A resolveAttributes(K key);

	protected CompletionStage<Void> prepareToResolveAttributes(List<Object> results, KeyFunction keyFunction, AttributesFunction attributesFunction) {
		return Stages.of(null);
	}

	private CompletionStage<Void> doResolveAttributes(List<Object> results, KeyFunction keyFunction, AttributesFunction attributesFunction) {
		for (Object result : results) {
			K key = toKey(keyFunction.extractKey(result));
			A attributes = resolveAttributes(key);
			if (attributes != null) {
				attributesFunction.applyAttributes(result, toAttributes(attributes));
			}
		}
		return Stages.of(null);
	}

	@Override
	public final CompletionStage<Void> resolveAttributes(final List<Object> results, final KeyFunction keyFunction, final AttributesFunction attributesFunction) {
		return prepareToResolveAttributes(results, keyFunction, attributesFunction).thenCompose($ ->
				doResolveAttributes(results, keyFunction, attributesFunction));
	}

}
