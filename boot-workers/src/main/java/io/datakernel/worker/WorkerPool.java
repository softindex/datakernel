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
import io.datakernel.di.annotation.ShortTypeName;
import io.datakernel.di.binding.BindingInfo;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.datakernel.di.binding.BindingType.TRANSIENT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

public final class WorkerPool {
	private final int id;
	private final Scope scope;
	private final Injector[] scopeInjectors;
	private final Map<Key<?>, BindingInfo> scopeBindings;

	@ShortTypeName("WorkerInstances")
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

	WorkerPool(Injector injector, int id, Scope scope, int workers) {
		this.id = id;
		this.scope = scope;
		this.scopeInjectors = new Injector[workers];

		Trie<Scope, Map<Key<?>, BindingInfo>> subtrie = injector.getBindingsTrie().get(scope);
		this.scopeBindings = subtrie != null ? subtrie.get() : emptyMap();

		for (int i = 0; i < workers; i++) {
			scopeInjectors[i] = injector.enterScope(scope);
			scopeInjectors[i].putInstance(Key.of(int.class, WorkerId.class), i);
			scopeInjectors[i].createEagerInstances();
		}
	}

	public int getId() {
		return id;
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
		Object[] instances = new Object[scopeInjectors.length];
		for (int i = 0; i < scopeInjectors.length; i++) {
			instances[i] = scopeInjectors[i].getInstance(key);
		}
		return new Instances<>(instances);
	}

	@Nullable
	public <T> Instances<T> peekInstances(Class<T> type) {
		return peekInstances(Key.of(type));
	}

	@Nullable
	public <T> Instances<T> peekInstances(Key<T> key) {
		BindingInfo binding = scopeBindings.get(key);
		if (binding == null || binding.getType() == TRANSIENT) {
			return null;
		}
		Object[] instances = doPeekInstances(key);
		if (Arrays.stream(instances).anyMatch(Objects::isNull)) {
			return null;
		}
		return new Instances<>(instances);
	}

	@NotNull
	public Map<Key<?>, Instances<?>> peekInstances() {
		Map<Key<?>, Instances<?>> map = new HashMap<>();
		for (Map.Entry<Key<?>, BindingInfo> entry : scopeBindings.entrySet()) {
			if (entry.getValue().getType() == TRANSIENT) {
				continue;
			}
			Object[] instances = doPeekInstances(entry.getKey());
			if (Arrays.stream(instances).noneMatch(Objects::isNull)) {
				map.put(entry.getKey(), new Instances<>(instances));
			}
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
		return "WorkerPool{scope=" + scope + (id > 0 ? "id=" + id : "") + "}";
	}
}
