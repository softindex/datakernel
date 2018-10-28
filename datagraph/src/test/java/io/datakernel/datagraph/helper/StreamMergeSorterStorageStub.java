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

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
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
	public Promise<Integer> newPartitionId() {
		int newPartition = partition++;
		return Promise.of(newPartition);
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		List<T> list = new ArrayList<>();
		storage.put(partition, list);
		StreamConsumerToList<T> consumer = StreamConsumerToList.create(list);
		return Promise.of(consumer.withLateBinding());
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		List<T> iterable = storage.get(partition);
		StreamSupplier<T> supplier = StreamSupplier.ofIterable(iterable);
		return Promise.of(supplier.withLateBinding());
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		for (Integer partition : partitionsToDelete) {
			storage.remove(partition);
		}
		return Promise.complete();
	}
}
