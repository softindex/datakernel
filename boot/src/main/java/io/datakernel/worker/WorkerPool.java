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

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public final class WorkerPool {
	private final Scope scope;
	private final Injector[] scopeInjectors;

	@SuppressWarnings("unchecked")
	public static final class Instances<T> implements Iterable<T> {
		private final Object[] instances;
		private final List<T> list;

		private Instances(Object[] instances) {
			this.instances = instances;
			this.list = (List<T>) asList(instances);
		}

		public Object[] getArray() {
			return instances;
		}

		public List<T> getList() {
			return list;
		}

		public T get(int i) {
			return (T) instances[i];
		}

		public int size() {
			return instances.length;
		}

		@Override
		public Iterator<T> iterator() {
			return list.iterator();
		}
	}

	WorkerPool(Injector injector, Scope scope, int workers) {
		this.scope = scope;
		this.scopeInjectors = new Injector[workers];
		for (int i = 0; i < workers; i++) {
			Map<Key<?>, Object> instances = new HashMap<>(singletonMap(Key.of(int.class, WorkerId.class), i));
			scopeInjectors[i] = injector.enterScope(scope, instances, false);
		}
	}

	public Scope getScope() {
		return scope;
	}

	@NotNull
	public <T> Instances<T> getInstances(Class<T> type) {
		return getInstances(Key.of(type));
	}

	@NotNull
	public <T> Instances<T> getInstances(Key<T> key) {
		Instances<T> instances = new Instances<>(new Object[scopeInjectors.length]);
		for (int i = 0; i < scopeInjectors.length; i++) {
			instances.instances[i] = scopeInjectors[i].getInstance(key);
		}
		return instances;
	}

	@Nullable
	public <T> Instances<T> peekInstances(Class<T> type) {
		return peekInstances(Key.of(type));
	}

	@Nullable
	public <T> Instances<T> peekInstances(Key<T> key) {
		if (!scopeInjectors[0].getBindings().get().containsKey(key)) return null;
		Object[] instances = doPeekInstances(key);
		if (Stream.of(instances).anyMatch(Objects::isNull)) return null;
		return new Instances<>(instances);
	}

	@NotNull
	public Map<Key<?>, Instances<?>> peekInstances() {
		Map<Key<?>, Instances<?>> map = new HashMap<>();
		Map<Key<?>, Binding<?>> bindings = scopeInjectors[0].getBindings().get();
		for (Key<?> key : scopeInjectors[0].peekInstances().keySet()) {
			if (!bindings.containsKey(key)) continue;
			Object[] instances = doPeekInstances(key);
			if (Stream.of(instances).anyMatch(Objects::isNull)) continue;
			map.put(key, new Instances<>(instances));
		}
		return map;
	}

	private Object[] doPeekInstances(Key<?> key) {
		Object[] instances = new Object[getSize()];
		for (int i = 0; i < instances.length; i++) {
			instances[i] = scopeInjectors[i].peekInstance(key);
		}
		return instances;
	}

	public Injector[] getScopeInjectors() {
		return scopeInjectors;
	}

	public int getSize() {
		return scopeInjectors.length;
	}

	@Override
	public String toString() {
		return "WorkerPool{scope=" + scope + "}";
	}
}
