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

final class WorkerThreadsPoolScope implements Scope, WorkerThreadsPoolFactory {
	@Inject
	Injector injector;

	private final Map<String, WorkerThreadsPoolImpl> pools = new HashMap<>();

	private WorkerThreadsPoolImpl currentPool;

	@Override
	public WorkerThreadsPool createPool(int poolSize) {
		return createPool("", poolSize);
	}

	@Override
	public WorkerThreadsPool createPool(String poolName, int poolSize) {
		checkState(!pools.containsKey(poolName), "Pool with name '%s' has already been created, make sure it is @Singleton", poolName);
		WorkerThreadsPoolImpl poolImpl = new WorkerThreadsPoolImpl(poolName, poolSize);
		pools.put(poolName, poolImpl);
		return poolImpl;
	}

	protected List<?> getPoolInstances(Key<?> key) {
		return pools.get(((WorkerThread) key.getAnnotation()).poolName()).getPoolInstances(key);
	}

	@Override
	public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
		return new Provider<T>() {
			@SuppressWarnings("unchecked")
			@Override
			public T get() {
				if (key.getAnnotationType() == WorkerId.class) {
					return (T) currentPool.currentWorkerId;
				}
				checkArgument(key.getAnnotation() instanceof WorkerThread);
				WorkerThreadsPoolImpl pool = pools.get(((WorkerThread) key.getAnnotation()).poolName());
				checkArgument(pool != null);

				checkState(pool.currentWorkerId != null,
						"To create %s use %s", key, WorkerThreadsPool.class.getSimpleName());
				T[] instances = (T[]) pool.pool.get(key);
				if (instances == null) {
					instances = (T[]) new Object[pool.poolSize];
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
	private static final class WorkerThreadAnnotation implements WorkerThread {
		final String value;
		final String poolName;

		WorkerThreadAnnotation(String value, String poolName) {
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
			return WorkerThread.class;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof WorkerThread)) return false;

			WorkerThread that = (WorkerThread) o;
			return value.equals(that.value()) && poolName.equals(that.poolName());
		}

		@Override
		public int hashCode() {
			return ((127 * "value".hashCode()) ^ value.hashCode()) + ((127 * "poolName".hashCode()) ^ poolName.hashCode());
		}

		public String toString() {
			return "@" + WorkerThread.class.getName() + "(value=" + value + ", poolName=" + poolName + ")";
		}

	}

	private class WorkerThreadsPoolImpl implements WorkerThreadsPool {
		private final String poolName;
		private final int poolSize;

		private final Map<Key<?>, Object[]> pool = new HashMap<>();

		@Nullable
		private Integer currentWorkerId;

		public WorkerThreadsPoolImpl(String poolName, int poolSize) {
			this.poolName = poolName;
			this.poolSize = poolSize;
		}

		@Override
		public String getPoolName() {
			return poolName;
		}

		@Override
		public int getPoolSize() {
			return poolSize;
		}

		@Override
		public <T> List<T> getPoolInstances(Class<T> type) {
			return getPoolInstances(Key.get(type, WorkerThread.class));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> List<T> getPoolInstances(TypeToken<T> type) {
			return getPoolInstances((Key<T>) Key.get(type.getType(), WorkerThread.class));
		}

		@Override
		public <T> List<T> getPoolInstances(Class<T> type, String instanceName) {
			return getPoolInstances(Key.get(type, new WorkerThreadAnnotation(instanceName, poolName)));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> List<T> getPoolInstances(TypeToken<T> type, String instanceName) {
			return getPoolInstances((Key<T>) Key.get(type.getType(), new WorkerThreadAnnotation(instanceName, poolName)));
		}

		private <T> List<T> getPoolInstances(Key<T> key) {
			checkArgument(key.getAnnotation() instanceof WorkerThread);
			WorkerThreadsPoolImpl originalPool = currentPool;
			currentPool = this;
			Integer originalPoolId = currentWorkerId;
			List<T> result = new ArrayList<>();
			for (int i = 0; i < poolSize; i++) {
				currentWorkerId = i;
				result.add(injector.getInstance(key));
			}
			currentWorkerId = originalPoolId;
			currentPool = originalPool;
			return result;
		}
	}
}
