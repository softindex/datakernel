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

package io.datakernel.aggregation_db;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.processor.StreamMergeSorterStorage;
import io.datakernel.stream.processor.StreamMergeSorterStorageImpl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("unchecked")
public final class SorterStorageUtils {
	public static <T> StreamMergeSorterStorage getSorterStorage(Eventloop eventloop, ExecutorService executorService,
	                                                            int blockSize, AggregationStructure structure,
	                                                            Class<T> recordClass, String sorterStorageDirectory,
	                                                            List<String> keys, List<String> fields) {
		Path path = Paths.get(sorterStorageDirectory, "%d.part");

		BufferSerializer bufferSerializer = structure.createBufferSerializer(recordClass, keys, fields);

		return new StreamMergeSorterStorageImpl(eventloop, executorService, bufferSerializer, path, blockSize);
	}

	public static <T> StreamMergeSorterStorage getSorterStorage(Eventloop eventloop, AggregationStructure structure,
	                                                            Class<T> recordClass, List<String> keys,
	                                                            List<String> fields) {
		return getSorterStorage(eventloop, Executors.newCachedThreadPool(), 64, structure, recordClass, "sorterStorage",
				keys, fields);
	}
}
