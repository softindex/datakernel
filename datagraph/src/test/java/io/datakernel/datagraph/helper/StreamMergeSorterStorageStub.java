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

package io.datakernel.datagraph.helper;

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.processor.StreamSorterStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamMergeSorterStorageStub<T> implements StreamSorterStorage<T> {
	protected final Eventloop eventloop;
	protected final Map<Integer, List<T>> storage = new HashMap<>();
	protected int partition;

	public StreamMergeSorterStorageStub(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Override
	public Stage<StreamConsumerWithResult<T, Integer>> write() {
		List<T> list = new ArrayList<>();
		int newPartition = partition++;
		storage.put(newPartition, list);
		StreamConsumerToList<T> consumer = StreamConsumerToList.create(list);
		return Stage.of(consumer.withResult(Stage.of(newPartition)).withLateBinding());
	}

	@Override
	public Stage<StreamProducerWithResult<T, Void>> read(int partition) {
		List<T> iterable = storage.get(partition);
		StreamProducer<T> producer = StreamProducer.ofIterable(iterable);
		return Stage.of(producer.withEndOfStreamAsResult().withLateBinding());
	}

	@Override
	public Stage<Void> cleanup(List<Integer> partitionsToDelete) {
		for (Integer partition : partitionsToDelete) {
			storage.remove(partition);
		}
		return Stage.complete();
	}
}
