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

package io.datakernel.datagraph.graph;

import io.datakernel.datagraph.server.DatagraphEnvironment;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

/**
 * Represents a context of a datagraph system: environment, producers and consumers.
 * Provides functionality to alter context and wire components.
 */
public final class TaskContext {
	private final DatagraphEnvironment environment;

	private final Eventloop eventloop;

	private final Map<StreamId, StreamProducer<?>> producers = new LinkedHashMap<>();
	private final Map<StreamId, StreamConsumer<?>> consumers = new LinkedHashMap<>();

	public TaskContext(Eventloop eventloop, DatagraphEnvironment environment) {
		this.environment = environment;
		this.eventloop = eventloop;
	}

	public DatagraphEnvironment environment() {
		return environment;
	}

	public Eventloop getEventloop() {
		return eventloop;
	}

	public <T> void bindChannel(StreamId streamId, StreamConsumer<T> consumer) {
		checkState(!consumers.containsKey(streamId));
		consumers.put(streamId, consumer);
	}

	public <T> void export(StreamId streamId, StreamProducer<T> producer) {
		checkState(!producers.containsKey(streamId));
		producers.put(streamId, producer);
	}

	@SuppressWarnings("unchecked")
	public void wireAll() {
		for (StreamId streamId : producers.keySet()) {
			StreamProducer<Object> producer = (StreamProducer<Object>) producers.get(streamId);
			StreamConsumer<Object> consumer = (StreamConsumer<Object>) consumers.get(streamId);
			checkNotNull(producer);
			checkNotNull(consumer, "Consumer not found for %s , producer %s", streamId, producer);
			producer.streamTo(consumer);
		}
	}
}