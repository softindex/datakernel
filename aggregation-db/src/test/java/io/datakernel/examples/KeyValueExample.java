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

package io.datakernel.examples;

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.FieldTypeInt;
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;

/**
 * In this example we will demonstrate how to build a key-value store on top of AggregationDB.
 */
public class KeyValueExample {
	private static final String DATA_PATH = "test/data/";

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ProcessorFactory keyValueProcessorFactory = new KeyValueProcessorFactory(classLoader);

		// to simplify this example we will just use a no-op metadata storage
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();

		// define the index structure (names and types of record properties) and metadata
		AggregationMetadata aggregationMetadata = new AggregationMetadata("key-value",
				KeyValuePair.KEYS, KeyValuePair.FIELDS);
		AggregationStructure aggregationStructure = new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("key", new KeyTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("value", new FieldTypeInt())
						.put("timestamp", new FieldTypeLong())
						.build());

		// local file system storage for data
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executorService,
				aggregationStructure, Paths.get(DATA_PATH));

		Aggregation aggregation = new Aggregation(eventloop, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, aggregationMetadata, aggregationStructure, keyValueProcessorFactory);

		// first chunk of data
		System.out.println("Input data:");
		List<KeyValuePair> firstList = asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2));
		System.out.println(firstList);
		StreamProducers.ofIterable(eventloop, firstList).streamTo(aggregation.consumer(KeyValuePair.class));
		eventloop.run();

		// second chunk of data
		List<KeyValuePair> secondList = asList(new KeyValuePair(2, 1, 3), new KeyValuePair(2, 2, 4),
				new KeyValuePair(2, 15, 5));
		System.out.println(secondList);
		StreamProducers.ofIterable(eventloop, secondList).streamTo(aggregation.consumer(KeyValuePair.class));
		eventloop.run();

		// third chunk of data
		List<KeyValuePair> thirdList = asList(new KeyValuePair(3, 4, 6), new KeyValuePair(3, 6, 7),
				new KeyValuePair(1, 0, 8));
		System.out.println(thirdList);
		StreamProducers.ofIterable(eventloop, thirdList).streamTo(aggregation.consumer(KeyValuePair.class));
		eventloop.run();

		// fourth chunk of data
		List<KeyValuePair> fourthList = asList(new KeyValuePair(1, 15, 9), new KeyValuePair(2, 21, 10));
		System.out.println(fourthList);
		StreamProducers.ofIterable(eventloop, fourthList).streamTo(aggregation.consumer(KeyValuePair.class));
		eventloop.run();

		// Perform the query. We will just retrieve records for all keys.
		AggregationQuery query = new AggregationQuery()
				.keys(KeyValuePair.KEYS)
				.fields(KeyValuePair.FIELDS);
		StreamConsumers.ToList<KeyValuePair> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		aggregation.query(query, KeyValuePair.class).streamTo(consumerToList);
		eventloop.run();

		System.out.println("Query result:");
		System.out.println(consumerToList.getList());

		executorService.shutdown();
	}
}
