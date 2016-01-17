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

package io.datakernel.boot;

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import io.datakernel.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

final class WorkerPoolScope implements Scope {
	final Map<Key<?>, WorkerPoolObjects> pool = new HashMap<>();

	@Nullable
	WorkerPool currentWorkerPool;

	@Nullable
	Integer currentWorkerId;

	private static class WorkerPoolObjects {
		private final WorkerPool workerPool;
		private final Object[] objects;

		public WorkerPoolObjects(WorkerPool workerPool, Object[] objects) {
			this.workerPool = workerPool;
			this.objects = objects;
		}
	}

	@Override
	public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
		return new Provider<T>() {
			@SuppressWarnings("unchecked")
			@Override
			public T get() {
				checkState(currentWorkerPool != null && currentWorkerId != null,
						"Use WorkerPool to get instances of %s", key);

				WorkerPoolObjects workerPoolObjects = pool.get(key);
				if (workerPoolObjects == null) {
					workerPoolObjects = new WorkerPoolObjects(currentWorkerPool, new Object[currentWorkerPool.workers]);
					pool.put(key, workerPoolObjects);
				}
				checkArgument(workerPoolObjects.workerPool == currentWorkerPool,
						"%s has been created with different WorkerPool", key);
				T[] instances = (T[]) workerPoolObjects.objects;
				T instance = instances[currentWorkerId];
				if (instance == null) {
					instance = unscoped.get();
					instances[currentWorkerId] = instance;
				}
				return instance;
			}
		};
	}

	List<?> getInstances(Key<?> key) {
		return Arrays.asList(pool.get(key).objects);
	}
}
