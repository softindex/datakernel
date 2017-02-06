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

import com.google.inject.*;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.spi.BindingScopingVisitor;
import com.google.inject.spi.ProvisionListener;

import java.lang.annotation.Annotation;

public final class WorkerPoolModule extends AbstractModule {
	private WorkerPoolScope workerPoolScope;

	public static boolean isWorkerScope(Binding<?> binding) {
		return binding.acceptScopingVisitor(new BindingScopingVisitor<Boolean>() {
			public Boolean visitNoScoping() {
				return false;
			}

			public Boolean visitScopeAnnotation(Class<? extends Annotation> visitedAnnotation) {
				return visitedAnnotation == Worker.class;
			}

			public Boolean visitScope(Scope visitedScope) {
				return visitedScope.getClass() == WorkerPoolScope.class;
			}

			public Boolean visitEagerSingleton() {
				return false;
			}
		});
	}

	@Override
	protected void configure() {
		workerPoolScope = new WorkerPoolScope();

		final Provider<Injector> injectorProvider = getProvider(Injector.class);
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return binding.getKey().getTypeLiteral().getRawType() == WorkerPool.class;
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				WorkerPool workerPool = (WorkerPool) provision.provision();
				workerPool.injector = injectorProvider.get();
				workerPool.poolScope = workerPoolScope;
			}
		});

		bindScope(Worker.class, workerPoolScope);
		bind(Integer.class).annotatedWith(WorkerId.class).toProvider(new Provider<Integer>() {
			@Override
			public Integer get() {
				return workerPoolScope.currentWorkerId;
			}
		});
	}

	public WorkerPoolObjects getPoolObjects(Key<?> key) {
		return workerPoolScope.getWorkerPoolObjects(key);
	}

	@Provides
	WorkerPools workerPools() {
		return workerPoolScope;
	}
}
