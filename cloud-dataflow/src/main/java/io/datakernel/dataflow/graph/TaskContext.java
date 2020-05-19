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

package io.datakernel.dataflow.graph;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.di.ResourceLocator;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.dataflow.di.DatasetIdImpl.datasetId;
import static java.util.stream.Collectors.toList;

/**
 * Represents a context of a datagraph system: environment, suppliers and consumers.
 * Provides functionality to alter context and wire components.
 */
public final class TaskContext {
	private final Map<StreamId, StreamSupplier<?>> suppliers = new LinkedHashMap<>();
	private final Map<StreamId, StreamConsumer<?>> consumers = new LinkedHashMap<>();
	private final SettablePromise<Void> executionPromise = new SettablePromise<>();

	private final ResourceLocator environment;

	public TaskContext(ResourceLocator environment) {
		this.environment = environment;
	}

	public Object get(String key) {
		return environment.getInstance(datasetId(key));
	}

	public <T> T get(Class<T> cls) {
		return environment.getInstance(cls);
	}

	public <T> void bindChannel(StreamId streamId, StreamConsumer<T> consumer) {
		checkState(!consumers.containsKey(streamId), "Already bound");
		consumers.put(streamId, consumer);
	}

	public <T> void export(StreamId streamId, StreamSupplier<T> supplier) {
		checkState(!suppliers.containsKey(streamId), "Already exported");
		suppliers.put(streamId, supplier);
	}

	@SuppressWarnings("unchecked")
	public Promise<Void> execute() {
		return Promises.all(suppliers.keySet().stream().map(streamId -> {
			try {
				StreamSupplier<Object> supplier = (StreamSupplier<Object>) suppliers.get(streamId);
				StreamConsumer<Object> consumer = (StreamConsumer<Object>) consumers.get(streamId);
				checkNotNull(supplier, "Supplier not found for %s , consumer %s", streamId, consumer);
				checkNotNull(consumer, "Consumer not found for %s , supplier %s", streamId, supplier);
				return supplier.streamTo(consumer);
			} catch (Exception e) {
				return Promise.ofException(e);
			}
		}).collect(toList())).whenComplete(executionPromise);
	}

	public void cancel() {
		suppliers.values().forEach(StreamSupplier::close);
		consumers.values().forEach(StreamConsumer::close);
	}

	public Promise<Void> getExecutionPromise() {
		return executionPromise;
	}

	public boolean isExecuted() {
		return executionPromise.isComplete();
	}
}
