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
import com.google.inject.Scope;
import io.datakernel.annotation.Nullable;

import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkState;

public final class WorkerPoolScope implements Scope {
	@Nullable
	private WorkerPool currentWorkerPool;
	@Nullable
	private Integer currentWorkerId = -1;

	@SuppressWarnings("unchecked")
	@Override
	public <T> Provider<T> scope(Key<T> key, Provider<T> unscoped) {
		return () -> {
			checkState(currentWorkerPool != null && currentWorkerId != null, "Use WorkerPool to get instances of %s", key);
			return (T) currentWorkerPool.getOrAdd(key, currentWorkerId, unscoped);
		};
	}

	public synchronized <T> T inScope(WorkerPool workerPool, int workerId, Supplier<T> supplier) {
		int originalWorkerId = currentWorkerId;
		WorkerPool originalWorkerPool = currentWorkerPool;
		currentWorkerId = workerId;
		currentWorkerPool = workerPool;
		T t = supplier.get();
		currentWorkerId = originalWorkerId;
		currentWorkerPool = originalWorkerPool;
		return t;
	}

	@Nullable
	public Integer getCurrentWorkerId() {
		return currentWorkerId;
	}

	@Nullable
	public WorkerPool getCurrentWorkerPool() {
		return currentWorkerPool;
	}
}
