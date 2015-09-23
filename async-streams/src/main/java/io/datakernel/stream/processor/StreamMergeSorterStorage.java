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

import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

/**
 * This class uses for  splitting a single input stream into smaller partitions during merge sort,
 * for avoid overflow RAM, it write it to  external memory . You can write here data with index
 * of partition and then read it from here and merge.
 *
 * @param <T> type of storing data
 */
public interface StreamMergeSorterStorage<T> {
	/**
	 * Method for creating consumer for writing to storage partition of elements
	 *
	 * @return consumer for streaming to storage
	 */
	// TODO (vsavchuk) make CompletionCallback previos other version?
	StreamConsumer<T> streamWriter();

	/**
	 * Method for creating producer for reading from storage partition of elements
	 *
	 * @param partition index of partition
	 * @return producer for streaming to storage
	 */
	StreamProducer<T> streamReader(int partition);

	/**
	 * Method for removing all stored created objects
	 */
	void cleanup();

	/**
	 * Returns next partition which will be filled for this storage
	 *
	 * @return next partition which will be filled
	 */
	int nextPartition();
}
