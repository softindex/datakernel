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
import com.google.inject.TypeLiteral;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

public final class WorkerPool {
	final int workers;

	WorkerPoolScope poolScope;

	Injector injector;

	public WorkerPool(int workers) {
		this.workers = workers;
	}

	public <T> List<T> getInstances(Class<T> type) {
		return getInstances(Key.get(type));
	}

	public int getWorkersCount() {
		return workers;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getInstances(TypeLiteral<T> type) {
		return getInstances(Key.get(type));
	}

	public <T> List<T> getInstances(Key<T> key) {
		checkState(injector != null && poolScope != null, "WorkerPool has not been initialized, make sure Boot module and ServiceGraph is used");
		checkArgument(injector.getExistingBinding(key) != null, "Binding for %s not found", key);
		WorkerPool originalWorkerPool = poolScope.currentWorkerPool;
		poolScope.currentWorkerPool = this;
		Integer originalWorkerId = poolScope.currentWorkerId;
		List<T> result = new ArrayList<>();
		for (int i = 0; i < workers; i++) {
			poolScope.currentWorkerId = i;
			result.add(injector.getInstance(key));
		}
		poolScope.currentWorkerId = originalWorkerId;
		poolScope.currentWorkerPool = originalWorkerPool;
		return result;
	}

}
