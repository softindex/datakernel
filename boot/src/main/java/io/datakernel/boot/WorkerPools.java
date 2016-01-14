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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import io.datakernel.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

public final class WorkerPools {
	Injector injector;

	private static class WorkerPool {
		private final String name;
		private final int workers;

		private final Map<Key<?>, Object[]> pool = new HashMap<>();

		public WorkerPool(String name, int workers) {
			this.name = name;
			this.workers = workers;
		}

	}

	private final Map<String, WorkerPool> pools = new HashMap<>();

	@Nullable
	WorkerPool currentWorkerPool;

	@Nullable
	Integer currentWorkerId;

	public static WorkerPools createDefaultPool(int workers) {
		WorkerPools workerPools = new WorkerPools();
		workerPools.addDefaultPool(workers);
		return workerPools;
	}

	public static WorkerPools createNamedPool(String poolName, int workers) {
		WorkerPools workerPools = new WorkerPools();
		workerPools.addNamedPool(poolName, workers);
		return workerPools;
	}

	private WorkerPools() {
	}

	public WorkerPools addDefaultPool(int workers) {
		return addNamedPool("", workers);
	}

	public WorkerPools addNamedPool(String poolName, int workers) {
		checkState(injector == null, "Cannot add pool, WorkerPool has already been initialized");
		checkState(!pools.containsKey(poolName), "Pool with name '%s' has already been created, make sure it is @Singleton", poolName);
		WorkerPool workerPool = new WorkerPool(poolName, workers);
		pools.put(poolName, workerPool);
		return this;
	}

	public <T> List<T> getInstances(Class<T> type) {
		return getInstances("", type);
	}

	public <T> List<T> getInstances(TypeToken<T> type) {
		return getInstances("", type);
	}

	public <T> List<T> getInstances(Class<T> type, String instanceName) {
		return getInstances("", type, instanceName);
	}

	public <T> List<T> getInstances(TypeToken<T> type, String instanceName) {
		return getInstances("", type, instanceName);
	}

	public <T> List<T> getInstances(String poolName, Class<T> type) {
		return getInstances(Key.get(type, new WorkerAnnotation("", poolName)));
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getInstances(String poolName, TypeToken<T> type) {
		return getInstances((Key<T>) Key.get(type.getType(), new WorkerAnnotation("", poolName)));
	}

	public <T> List<T> getInstances(String poolName, Class<T> type, String instanceName) {
		return getInstances(Key.get(type, new WorkerAnnotation(instanceName, poolName)));
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getInstances(String poolName, TypeToken<T> type, String instanceName) {
		return getInstances((Key<T>) Key.get(type.getType(), new WorkerAnnotation(instanceName, poolName)));
	}

	public <T> List<T> getInstances(Key<T> key) {
		checkState(injector != null, "WorkerPools has not been initialized, make sure Boot module and ServiceGraph is used");
		checkArgument(key.getAnnotation() instanceof Worker, "Can only get @Worker instances, got key: %s", key);
		WorkerPool pool = pools.get(((Worker) key.getAnnotation()).poolName());
		WorkerPool originalWorkerPool = currentWorkerPool;
		currentWorkerPool = pool;
		Integer originalWorkerId = currentWorkerId;
		List<T> result = new ArrayList<>();
		for (int i = 0; i < pool.workers; i++) {
			currentWorkerId = i;
			result.add(injector.getInstance(key));
		}
		currentWorkerId = originalWorkerId;
		currentWorkerPool = originalWorkerPool;
		return result;
	}

	@SuppressWarnings("unchecked")
	<T> T provideInstance(Key<T> key, Provider<T> unscoped) {
		checkArgument(key.getAnnotation() instanceof Worker);
		checkState(currentWorkerPool != null);
		checkState(currentWorkerId != null);
		checkArgument(((Worker) key.getAnnotation()).poolName().equals(currentWorkerPool.name));

		T[] instances = (T[]) currentWorkerPool.pool.get(key);
		if (instances == null) {
			instances = (T[]) new Object[currentWorkerPool.workers];
			currentWorkerPool.pool.put(key, instances);
		}
		T instance = instances[currentWorkerId];
		if (instance == null) {
			instance = unscoped.get();
			instances[currentWorkerId] = instance;
		}
		return instance;
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

}
