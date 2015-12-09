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

package io.datakernel.guice.workers;

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import io.datakernel.guice.servicegraph.ServiceGraphModule;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public final class NioWorkerScope implements Scope, NioWorkerScopeFactory {
	private List<Map<Key<?>, Object>> pool;
	private Map<Key<?>, List<ServiceGraphModule.KeyInPool>> mapKeys;
	private int currentPool = -1;

	private final Provider<Integer> numberProvider = new Provider<Integer>() {
		@Override
		public Integer get() {
			return getCurrentPool();
		}
	};

	public int getCurrentPool() {
		return currentPool;
	}

	@Override
	public <T> List<T> getList(int size, Provider<T> itemProvider) {
		if (pool == null) {
			pool = initPool(size);
			mapKeys = new HashMap<>();
		} else {
			if (pool.size() != size) {
				throw new IllegalArgumentException("Pool cannot have different size: old size = " + pool.size() + ", new size: " + size);
			}
		}

		int safeCurrentPool = currentPool;
		if (pool == null)
			throw new IllegalStateException("Scope is not initialized");
		List<T> result = new ArrayList<>(pool.size());
		for (int i = 0; i < pool.size(); i++) {
			currentPool = i;
			result.add(itemProvider.get());
		}
		currentPool = safeCurrentPool;
		return result;
	}

	@Override
	public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
		return new Provider<T>() {
			@Override
			public T get() {
				checkScope(key);
				Map<Key<?>, Object> scopedObjects = pool.get(currentPool);
				@SuppressWarnings("unchecked")
				T current = (T) scopedObjects.get(key);
				if (current == null && !scopedObjects.containsKey(key)) {
					current = unscoped.get();
					checkNioSingleton(key, current);
					scopedObjects.put(key, current);
					if (!mapKeys.containsKey(key))
						mapKeys.put(key, new ArrayList<ServiceGraphModule.KeyInPool>());
					mapKeys.get(key).add(new ServiceGraphModule.KeyInPool(key, currentPool));
				}
				return current;
			}
		};
	}

	public List<Map<Key<?>, Object>> getPool() {
		if (pool == null)
			return Collections.emptyList();
		else
			return new ArrayList<>(pool);
	}

	public Map<Key<?>, List<ServiceGraphModule.KeyInPool>> getMapKeys() {
		if (mapKeys == null) {
			return Collections.emptyMap();
		} else {
			return new HashMap<>(mapKeys);
		}
	}

	private <T> void checkScope(Key<T> key) {
		if (currentPool < 0 || currentPool > pool.size())
			throw new RuntimeException("Could not bind " + ((key.getAnnotation() == null) ? "" :
					"@" + key.getAnnotation() + " ") + key.getTypeLiteral() + " in NioPoolScope.");
	}

	private <T> void checkNioSingleton(Key<T> key, T object) {
		for (int i = 0; i < pool.size(); i++) {
			if (i == currentPool) continue;
			Map<Key<?>, Object> scopedObjects = pool.get(i);
			Object o = scopedObjects.get(key);
			if (o != null && o == object)
				throw new IllegalStateException("Provider must returns NioSingleton object of " + object.getClass());
		}
	}

	public Provider<Integer> getNumberScopeProvider() {
		return numberProvider;
	}

	private static List<Map<Key<?>, Object>> initPool(int size) {
		checkArgument(size > 0, "size must be positive value, got %s", size);
		List<Map<Key<?>, Object>> pool = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			pool.add(new HashMap<Key<?>, Object>());
		}
		return pool;
	}

}
