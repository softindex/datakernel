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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

public final class WorkerPool {
	private final int workers;
	private String annotationString = "";

	private WorkerPoolScope poolScope;
	private Injector injector;

	final Map<Key<?>, List<Object>> pool = new ConcurrentHashMap<>();

	private final ThreadLocal<Integer> threadLocalWorkerId = new ThreadLocal<>();

	private final Map<Key<?>, ThreadLocal<?>> threadLocalCache = new ConcurrentHashMap<>();

	public WorkerPool(int workers) {
		this.workers = workers;
	}

	@Nullable
	Integer getLocalWorkerId() {
		Integer workerId = threadLocalWorkerId.get();
		if (workerId != null) {
			return workerId;
		}
		Optional<? extends List<?>> eventloops = pool.entrySet()
				.stream()
				.filter(entry -> entry.getKey().getTypeLiteral().getRawType().equals(Eventloop.class))
				.map(Entry::getValue)
				.findAny();
		checkState(eventloops.isPresent(), "No eventloops in worker pool!");
		int index = eventloops.get().indexOf(Eventloop.getCurrentEventloop());
		if (index == -1) {
			return null;
		}
		threadLocalWorkerId.set(index);
		return index;
	}

	@SuppressWarnings("unchecked")
	public <T> T getCurrentInstance(Key<T> key) {
		Integer localWorkerId = getLocalWorkerId();
		checkState(localWorkerId != null, "No instance of %s is associated with current thread", key);

		return getInstances(key).get(localWorkerId);
	}

	// region getCurrentInstance overloads
	public <T> T getCurrentInstance(Class<T> type) {
		return getCurrentInstance(Key.get(type));
	}

	public <T> T getCurrentInstance(TypeLiteral<T> type) {
		return getCurrentInstance(Key.get(type));
	}
	// endregion

	@SuppressWarnings("unchecked")
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
	public synchronized <T> List<T> getInstances(Key<T> key) {
		checkState(injector != null && poolScope != null, "WorkerPool has not been initialized, make sure Boot module and ServiceGraph is used");
		checkArgument(injector.getExistingBinding(key) != null, "Binding for %s not found", key);


		List<T> result = (List<T>) pool.get(key);
		if (result != null) {
			return result;
		}
		result = new ArrayList<>();

		for (int i = 0; i < workers; i++) {
			result.add(poolScope.inScope(this, i, () -> injector.getInstance(key)));
		}

		return result;
	}

	// region getInstances overloads
	public <T> List<T> getInstances(Class<T> type) {
		return getInstances(Key.get(type));
	}

	public <T> List<T> getInstances(TypeLiteral<T> type) {
		return getInstances(Key.get(type));
	}
	// endregion

	public synchronized <T> List<T> getExistingInstances(Key<T> key) {
		checkState(injector != null && poolScope != null, "WorkerPool has not been initialized, make sure Boot module and ServiceGraph is used");
		checkArgument(injector.getExistingBinding(key) != null, "Binding for %s not found", key);
		return (List<T>) pool.get(key);
	}

	@SuppressWarnings("unchecked")
	Object getOrAdd(Key<?> key, int workerId, Provider<?> unscoped) {
		List<Object> instances = pool.computeIfAbsent(key, $ -> Arrays.asList(new Object[workers]));
		Object instance = instances.get(workerId);
		if (instance == null) {
			instance = unscoped.get();
			instances.set(workerId, instance);
		}
		return instance;
	}

	public int getWorkersCount() {
		return workers;
	}

	public void setScopeInstance(WorkerPoolScope poolScope) {
		this.poolScope = poolScope;
	}

	public void setAnnotationString(String annotationString){
		this.annotationString = annotationString;
	}

	public void setInjector(Injector injector) {
		this.injector = injector;
	}
	public boolean isValidBinding(Key<?> key) {
		return pool.get(key) != null || getInstances(key).size() != 0;
	}

	public String getAnnotationString() {
		return annotationString;
	}

	@Override
	public String toString() {
		return "WorkerPool" + annotationString;
	}
}
