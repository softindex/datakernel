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

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

final class WorkerPoolScope implements Scope {
	@Inject(optional = true)
	WorkerPools pools;

	@Override
	public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
		return new Provider<T>() {
			@SuppressWarnings("unchecked")
			@Override
			public T get() {
				if (key.getAnnotationType() == WorkerId.class) {
					return (T) pools.currentWorkerId;
				}
				return pools.provideInstance(key, unscoped);
			}
		};
	}

	List<?> getInstances(Key<?> key) {
		checkState(pools != null, "WorkerPools instance must be provided in Guice modules");
		return pools.getInstances(key);
	}
}