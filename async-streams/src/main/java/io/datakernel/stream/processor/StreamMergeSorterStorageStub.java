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

package io.datakernel.stream.processor;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamMergeSorterStorageStub<T> implements StreamMergeSorterStorage<T> {
	protected final Eventloop eventloop;
	protected final Map<Integer, List<T>> storage = new HashMap<>();
	protected int partition;

	public StreamMergeSorterStorageStub(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Override
	public void write(StreamProducer<T> producer, CompletionCallback completionCallback) {
		List<T> list = new ArrayList<>();
		storage.put(partition++, list);
		StreamConsumers.ToList<T> consumer = StreamConsumers.toList(eventloop, list);
		producer.streamTo(consumer);

		consumer.setCompletionCallback(completionCallback);
	}

	@Override
	public StreamProducer<T> read(int partition) {
		return StreamProducers.ofIterable(eventloop, storage.get(partition));
	}

	@Override
	public void cleanup() {
		storage.clear();
	}

	@Override
	public int nextPartition() {
		return partition;
	}
}
