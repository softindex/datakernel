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

package io.datakernel.worker;

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("ALL")
public final class WorkerPools {
	private final List<WorkerPool> workerPools = new CopyOnWriteArrayList<>();
	private final ThreadLocal<WorkerPool> threadLocalWorkerPool = ThreadLocal.withInitial(() ->
			workerPools.stream()
					// if local worker id exists than this thread is associated with a worker from this pool
					.filter(pool -> pool.getLocalWorkerId() != null)
					.findAny()
					.orElseThrow(() -> new IllegalStateException("No WorkerPool is associated with current thread")));

	private final Map<Key<?>, ThreadLocal<?>> threadLocalCache = new ConcurrentHashMap<>();

	void addWorkerPool(WorkerPool workerPool) {
		checkArgument(!workerPools.contains(workerPool), "WorkerPool has already been added");
		workerPools.add(workerPool);
	}

	public List<WorkerPool> getWorkerPools() {
		return Collections.unmodifiableList(workerPools);
	}

	public <T> Map<WorkerPool, List<T>> getWorkerPoolObjects(Key<T> key) {
		return workerPools.stream().collect(Collectors.toMap(Function.identity(), workerPool -> workerPool.getInstances(key)));
	}

	public <T> List<T> getAllObjects(Key<T> key) {
		return workerPools.stream().flatMap(workerPool -> workerPool.getInstances(key).stream()).collect(toList());
	}

	public WorkerPool getCurrentWorkerPool() {
		return threadLocalWorkerPool.get();
	}

	@SuppressWarnings("unchecked")
	public <T> T getCurrentInstance(Key<T> key) {
		return getCurrentWorkerPool().getCurrentInstance(key);
	}

	// region getCurrentInstance overloads
	public <T> T getCurrentInstance(Class<T> type) {
		return getCurrentInstance(Key.get(type));
	}

	public <T> T getCurrentInstance(TypeLiteral<T> type) {
		return getCurrentInstance(Key.get(type));
	}
	// endregion

	public <T> Provider<T> getCurrentInstanceProvider(Key<T> key) {
		checkArgument(isValidBinding(key), "Cannot get provider for key: %s", key);
		ThreadLocal<T> threadLocal = (ThreadLocal<T>) threadLocalCache.computeIfAbsent(key, $ ->
				ThreadLocal.withInitial(() -> getCurrentInstance(key)));
		return threadLocal::get;
	}


	// region getCurrentInstanceProvider overloads
	public <T> Provider<T> getCurrentInstanceProvider(Class<T> type) {
		return getCurrentInstanceProvider(Key.get(type));
	}

	public <T> Provider<T> getCurrentInstanceProvider(TypeLiteral<T> type) {
		return getCurrentInstanceProvider(Key.get(type));
	}
	// endregion

	@SuppressWarnings("unchecked")
	public <T> List<T> getInstances(Key<T> key) {
		return getCurrentWorkerPool().getInstances(key);
	}

	// region getInstances overloads
	public <T> List<T> getInstances(Class<T> type) {
		return getInstances(Key.get(type));
	}

	public <T> List<T> getInstances(TypeLiteral<T> type) {
		return getInstances(Key.get(type));
	}
	// endregion

	private boolean isValidBinding(Key<?> key) {
		return workerPools.stream()
				.anyMatch(workerPool -> workerPool.isValidBinding(key));
	}
}
