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

package io.datakernel.dataflow.server;

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Environment for datagraph, which holds the instances of classes, required to perform certain datagraph operations.
 */
public final class DataflowEnvironment {
	@Nullable
	private final DataflowEnvironment parent;
	private final Map<?, ?> instances = new HashMap<>();

	private DataflowEnvironment(@Nullable DataflowEnvironment parent) {
		this.parent = parent;
	}

	/**
	 * Constructs a datagraph environment without a parent environment.
	 *
	 * @return new datagraph environment
	 */
	public static DataflowEnvironment create() {
		return new DataflowEnvironment(null);
	}

	/**
	 * Constructs a datagraph environment that extends the given datagraph environment. Instances defined in the parent environment are available in this newly constructed environment.
	 *
	 * @param parent parent environment to extend
	 * @return new datagraph environment with the specified parent environment
	 */
	public static DataflowEnvironment extend(DataflowEnvironment parent) {
		return new DataflowEnvironment(parent);
	}

	public DataflowEnvironment extend() {
		return extend(this);
	}

	/**
	 * Sets the given value for the specified key.
	 *
	 * @param key   key
	 * @param value value
	 * @return this environment
	 */
	@SuppressWarnings("unchecked")
	public DataflowEnvironment with(Object key, Object value) {
		((Map<Object, Object>) instances).put(key, value);
		return this;
	}

	/**
	 * Sets the specified instance for the given key (instance type).
	 *
	 * @param type  instance type
	 * @param value instance
	 * @param <T>   type of the instance
	 * @return this environment
	 */
	@SuppressWarnings("unchecked")
	public <T> DataflowEnvironment setInstance(Class<T> type, T value) {
		((Map<Class<T>, T>) instances).put(type, value);
		return this;
	}

	@Nullable
	public Object get(Object key) {
		Object result = instances.get(key);
		if (result == null && parent != null) {
			result = parent.get(key);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> T getInstance(Class<T> type) {
		T result = (T) instances.get(type);
		if (result == null && parent != null) {
			result = parent.getInstance(type);
		}
		return result;
	}
}
