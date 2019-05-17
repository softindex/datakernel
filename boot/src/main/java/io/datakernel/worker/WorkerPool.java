/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.util.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;

public final class WorkerPool {
	private final Scope scope;
	private final int idx;
	private final Injector[] scopeInjectors;

	WorkerPool(Injector injector, Scope scope, int idx, int workers) {
		this.scope = scope;
		this.idx = idx;
		this.scopeInjectors = new Injector[workers];
		for (int i = 0; i < workers; i++) {
			scopeInjectors[i] = injector.enterScope(scope);
		}
	}

	@NotNull
	public Scope getScope() {
		return scope;
	}

	@NotNull
	public <T> List<T> getInstances(Key<T> key) {
		List<T> instances = new ArrayList<>(scopeInjectors.length);
		for (Injector scopeInjector : scopeInjectors) {
			instances.add(scopeInjector.getInstance(key));
		}
		return instances;
	}

	@NotNull
	public <T> List<T> getInstances(Class<T> type) {
		return getInstances(Key.of(type));
	}

	@NotNull
	public <T> List<T> getInstances(TypeT<T> type) {
		return getInstances(Key.of(type));
	}

	@Nullable
	private <T> List<T> peekInstances(Class<T> type) {
		return peekInstances(Key.of(type));
	}

	@Nullable
	private <T> List<T> peekInstances(TypeT<T> type) {
		return peekInstances(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> List<T> peekInstances(Key<T> key) {
		if (!scopeInjectors[0].peekInstances().containsKey(key)) return null;
		return asList((T[]) doPeekInstances(key));
	}

	@NotNull
	public Map<Key<?>, Object[]> peekInstances() {
		return scopeInjectors[0].peekInstances().keySet()
				.stream()
				.collect(toMap(Function.identity(), this::doPeekInstances));
	}

	private Object[] doPeekInstances(Key<?> key) {
		Object[] instances = new Object[getSize()];
		for (int i = 0; i < instances.length; i++) {
			instances[i] = scopeInjectors[i].peekInstance(key);
		}
		return instances;
	}

	public List<Injector> getScopeInjectors() {
		return asList(scopeInjectors);
	}

	public int getSize() {
		return scopeInjectors.length;
	}

	@Override
	public String toString() {
		return "pool_" + idx;
	}
}
