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

import com.google.common.reflect.TypeToken;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public final class WorkerPool {
	final int workers;

	WorkerPoolScope poolScope;

	Injector injector;

	public WorkerPool(int workers) {
		this.workers = workers;
	}

	public <T> List<T> getInstances(Class<T> type) {
		return getInstances(Key.get(type, new WorkerAnnotation("")));
	}

	public int getWorkersCount() {
		return workers;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getInstances(TypeToken<T> type) {
		return getInstances((Key<T>) Key.get(type.getType(), new WorkerAnnotation("")));
	}

	public <T> List<T> getInstances(Class<T> type, String namedWorker) {
		return getInstances(Key.get(type, new WorkerAnnotation(namedWorker)));
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getInstances(TypeToken<T> type, String namedWorker) {
		return getInstances((Key<T>) Key.get(type.getType(), new WorkerAnnotation(namedWorker)));
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

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static final class WorkerAnnotation implements Worker {
		final String value;

		WorkerAnnotation(String value) {
			this.value = checkNotNull(value);
		}

		@Override
		public String value() {
			return value;
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return Worker.class;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Worker)) return false;

			Worker that = (Worker) o;
			return value.equals(that.value());
		}

		@Override
		public int hashCode() {
			return ((127 * "value".hashCode()) ^ value.hashCode());
		}

		public String toString() {
			return "@" + Worker.class.getName() + "(value=" + value + ")";
		}
	}

}
