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

import com.google.common.reflect.TypeToken;
import com.google.inject.*;
import io.datakernel.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

final class WorkerPoolScope implements Scope, WorkerPoolFactory {
	@Inject
	Injector injector;

	private final Map<String, WorkerPoolImpl> pools = new HashMap<>();

	private WorkerPoolImpl currentWorkerPool;

	@Override
	public WorkerPool createPool(int workers) {
		return createPool("", workers);
	}

	@Override
	public WorkerPool createPool(String name, int workers) {
		checkState(!pools.containsKey(name), "Pool with name '%s' has already been created, make sure it is @Singleton", name);
		WorkerPoolImpl workerPool = new WorkerPoolImpl(name, workers);
		pools.put(name, workerPool);
		return workerPool;
	}

	protected List<?> getPoolInstances(Key<?> key) {
		return pools.get(((Worker) key.getAnnotation()).poolName()).getInstances(key);
	}

	@Override
	public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
		return new Provider<T>() {
			@SuppressWarnings("unchecked")
			@Override
			public T get() {
				if (key.getAnnotationType() == WorkerId.class) {
					return (T) currentWorkerPool.currentWorkerId;
				}
				checkArgument(key.getAnnotation() instanceof Worker);
				WorkerPoolImpl pool = pools.get(((Worker) key.getAnnotation()).poolName());
				checkArgument(pool != null);

				checkState(pool.currentWorkerId != null,
						"To create %s use %s", key, WorkerPool.class.getSimpleName());
				T[] instances = (T[]) pool.pool.get(key);
				if (instances == null) {
					instances = (T[]) new Object[pool.workers];
					pool.pool.put(key, instances);
				}
				T instance = instances[pool.currentWorkerId];
				if (instance == null) {
					instance = unscoped.get();
					instances[pool.currentWorkerId] = instance;
				}
				return instance;
			}
		};
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static final class WorkerAnnotation implements Worker {
		final String value;
		final String poolName;

		WorkerAnnotation(String value, String poolName) {
			this.value = checkNotNull(value);
			this.poolName = checkNotNull(poolName);
		}

		@Override
		public String value() {
			return value;
		}

		@Override
		public String poolName() {
			return poolName;
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return Worker.class;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Worker)) return false;

			Worker that = (Worker) o;
			return value.equals(that.value()) && poolName.equals(that.poolName());
		}

		@Override
		public int hashCode() {
			return ((127 * "value".hashCode()) ^ value.hashCode()) + ((127 * "poolName".hashCode()) ^ poolName.hashCode());
		}

		public String toString() {
			return "@" + Worker.class.getName() + "(value=" + value + ", poolName=" + poolName + ")";
		}

	}

	private class WorkerPoolImpl implements WorkerPool {
		private final String name;
		private final int workers;

		private final Map<Key<?>, Object[]> pool = new HashMap<>();

		@Nullable
		private Integer currentWorkerId;

		public WorkerPoolImpl(String name, int workers) {
			this.name = name;
			this.workers = workers;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public int getWorkers() {
			return workers;
		}

		@Override
		public <T> List<T> getInstances(Class<T> type) {
			return getInstances(Key.get(type, Worker.class));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> List<T> getInstances(TypeToken<T> type) {
			return getInstances((Key<T>) Key.get(type.getType(), Worker.class));
		}

		@Override
		public <T> List<T> getInstances(Class<T> type, String instanceName) {
			return getInstances(Key.get(type, new WorkerAnnotation(instanceName, name)));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> List<T> getInstances(TypeToken<T> type, String instanceName) {
			return getInstances((Key<T>) Key.get(type.getType(), new WorkerAnnotation(instanceName, name)));
		}

		private <T> List<T> getInstances(Key<T> key) {
			checkArgument(key.getAnnotation() instanceof Worker);
			WorkerPoolImpl originalWorkerPool = currentWorkerPool;
			currentWorkerPool = this;
			Integer originalWorkerId = currentWorkerId;
			List<T> result = new ArrayList<>();
			for (int i = 0; i < workers; i++) {
				currentWorkerId = i;
				result.add(injector.getInstance(key));
			}
			currentWorkerId = originalWorkerId;
			currentWorkerPool = originalWorkerPool;
			return result;
		}
	}
}
