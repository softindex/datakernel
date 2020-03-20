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

package io.datakernel.launchers.dataflow;

import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.DataflowEnvironment;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamSorterStorage;
import io.datakernel.promise.Promise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamMergeSorterStorageStub<T> implements StreamSorterStorage<T> {

	public static final StreamSorterStorageFactory FACTORY_STUB = new StreamSorterStorageFactory() {
		@Override
		public <C> StreamSorterStorage<C> create(Class<C> type, DataflowEnvironment environment, Promise<Void> taskExecuted) {
			return new StreamMergeSorterStorageStub<>();
		}
	};

	private final Map<Integer, List<T>> storage = new HashMap<>();
	private int partition;

	private StreamMergeSorterStorageStub() {
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
		return Promise.of(consumer);
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		List<T> iterable = storage.get(partition);
		StreamSupplier<T> supplier = StreamSupplier.ofIterable(iterable);
		return Promise.of(supplier);
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		for (Integer partition : partitionsToDelete) {
			storage.remove(partition);
		}
		return Promise.complete();
	}
}
