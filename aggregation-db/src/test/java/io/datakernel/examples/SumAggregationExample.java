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
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation_db.fieldtype.FieldTypes.doubleSum;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.longSum;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static io.datakernel.aggregation_db.keytype.KeyTypes.longKey;
import static java.util.Arrays.asList;

/**
 * This example explains how to implement a sum aggregation using AggregationDB.
 */
public class SumAggregationExample {
	private static final String DATA_PATH = "test/data/";

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newCachedThreadPool();
		Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();

		// to simplify this example we will just use a no-op metadata storage
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();

		// define the index structure (names and types of record properties) and metadata
		AggregationMetadata aggregationMetadata = new AggregationMetadata(DataRecord.KEYS, DataRecord.FIELDS);
		AggregationStructure aggregationStructure = new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("key1", intKey())
						.put("key2", longKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("value1", longSum())
						.put("value2", doubleSum())
						.build());

		// local file system storage for data
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executorService,
				aggregationStructure, Paths.get(DATA_PATH));

		Aggregation aggregation = new Aggregation(eventloop, executorService, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, aggregationMetadata, aggregationStructure);

		// first chunk of data
		System.out.println("Input data:");
		List<DataRecord> firstList = asList(new DataRecord(1, 1, 11, 17.34), new DataRecord(1, 2, 3, 15.3),
				new DataRecord(1, 1, 42, 18.933));
		System.out.println(firstList);
		StreamProducers.ofIterable(eventloop, firstList).streamTo(aggregation.consumer(DataRecord.class));
		eventloop.run();

		// second chunk of data
		List<DataRecord> secondList = asList(new DataRecord(1, 3, 10, 15), new DataRecord(1, 2, -5, -21),
				new DataRecord(1, 1, 11, 11.333));
		System.out.println(secondList);
		StreamProducers.ofIterable(eventloop, secondList).streamTo(aggregation.consumer(DataRecord.class));
		eventloop.run();

		// third chunk of data
		List<DataRecord> thirdList = asList(new DataRecord(1, 1, 20, 7.7), new DataRecord(1, 3, -1, 20),
				new DataRecord(1, 4, 17, 42));
		System.out.println(thirdList);
		StreamProducers.ofIterable(eventloop, thirdList).streamTo(aggregation.consumer(DataRecord.class));
		eventloop.run();

		// Perform the query. We will just retrieve records for all keys.
		AggregationQuery query = new AggregationQuery()
				.keys(DataRecord.KEYS)
				.fields(DataRecord.FIELDS);
		StreamConsumers.ToList<DataRecord> consumerToList = StreamConsumers.toList(eventloop);
		aggregation.query(query, DataRecord.class).streamTo(consumerToList);
		eventloop.run();

		System.out.println("Query result:");
		System.out.println(consumerToList.getList());

		executorService.shutdown();
	}
}
