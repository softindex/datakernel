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

import io.datakernel.stream.StreamProducer;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * This class uses for  splitting a single input stream into smaller partitions during merge sort,
 * for avoid overflow RAM, it write it to  external memory . You can write here data with index
 * of partition and then read it from here and merge.
 *
 * @param <T> type of storing data
 */
public interface StreamMergeSorterStorage<T> {
	/**
	 * Method for writing to storage partition of elements
	 *
	 * @return partition number
	 */
	PartitionStage write(StreamProducer<T> producer);

	/**
	 * Method for creating producer for reading from storage partition of elements
	 *
	 * @param partition index of partition
	 * @return producer for streaming to storage
	 */
	ProducerStage<T> read(int partition);

	/**
	 * Method for removing all stored created objects
	 */
	void cleanup(List<Integer> partitionsToDelete);

	public static class PartitionStage {
		private final int partition;
		private final CompletionStage<Void> stage;

		public PartitionStage(int partition, CompletionStage<Void> stage) {
			this.partition = partition;
			this.stage = stage;
		}

		public int getPartition() {
			return partition;
		}

		public CompletionStage<Void> getStage() {
			return stage;
		}
	}

	public static class ProducerStage<T> {
		private final StreamProducer<T> producer;
		private final CompletionStage<Void> stage;

		public ProducerStage(StreamProducer<T> producer, CompletionStage<Void> stage) {
			this.producer = producer;
			this.stage = stage;
		}

		public StreamProducer<T> getProducer() {
			return producer;
		}

		public CompletionStage<Void> getStage() {
			return stage;
		}
	}

}
