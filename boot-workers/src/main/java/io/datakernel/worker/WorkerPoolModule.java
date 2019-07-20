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

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

public final class WorkerPoolModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(WorkerPools.class).to(WorkerPools::new, Injector.class);
		bind(Key.of(int.class, WorkerId.class)).in(Worker.class);

		generate(int.class, (bindings, scope, key) -> {
			if (scope.length == 0 || key.getName() == null || key.getName().getAnnotationType() != WorkerId.class) {
				return null;
			}
			return Binding.to(() -> {
				throw new IllegalStateException("Expected instance override for the worker id by Injector#enterScope call");
			});
		});
		generate(WorkerPool.Instances.class, (bindings, scope, key) -> {
			Key<Object> requestedKey = key.getTypeParameter(0);
			return Binding.to(wp -> wp.getInstances(requestedKey), Key.of(WorkerPool.class, key.getName()));
		});
	}
}
