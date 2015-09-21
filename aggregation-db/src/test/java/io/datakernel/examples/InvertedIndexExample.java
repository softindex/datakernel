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
import io.datakernel.aggregation_db.fieldtype.FieldTypeList;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeString;
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
 * AggregationDB can also be used to perform more complex aggregation. In this example we will explain how to build an inverted index.
 */
public class InvertedIndexExample {
	private static final String DATA_PATH = "test/data/";

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ProcessorFactory processorFactory = new InvertedIndexProcessorFactory(classLoader);

		// to simplify this example we will just use a no-op metadata storage
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();

		AggregationMetadata aggregationMetadata = new AggregationMetadata("inverted index", InvertedIndexRecord.KEYS,
				InvertedIndexRecord.INPUT_FIELDS, InvertedIndexQueryResult.OUTPUT_FIELDS);

		/* Define index structure. Here input records (which are streamed to DB)
		and output records (which are retrieved from DB) have different structure:
		Input object: { 'word' : String, 'documentId' : Integer }.
		Output object: { 'word' : String, 'documents' : List<Integer> }.
		 */
		AggregationStructure structure = new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("word", new KeyTypeString())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("documentId", new FieldTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("documents", new FieldTypeList(new FieldTypeInt()))
						.build(),
				ImmutableMap.<String, String>of());

		// local file system storage for data
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executorService,
				structure, Paths.get(DATA_PATH));

		Aggregation aggregation = new Aggregation(eventloop, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, aggregationMetadata, structure, processorFactory);

		// first chunk of data
		System.out.println("Input data:");
		List<InvertedIndexRecord> firstList = asList(new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3));
		System.out.println(firstList);
		StreamProducers.ofIterable(eventloop, firstList).streamTo(aggregation.consumer(InvertedIndexRecord.class));
		eventloop.run();

		// second chunk of data
		List<InvertedIndexRecord> secondList = asList(new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4), new InvertedIndexRecord("dog", 1));
		System.out.println(secondList);
		StreamProducers.ofIterable(eventloop, secondList).streamTo(aggregation.consumer(InvertedIndexRecord.class));
		eventloop.run();

		// third chunk of data
		List<InvertedIndexRecord> thirdList = asList(new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10));
		System.out.println(thirdList);
		StreamProducers.ofIterable(eventloop, thirdList).streamTo(aggregation.consumer(InvertedIndexRecord.class));
		eventloop.run();

		// Perform the query. We will just retrieve records for all keys.
		AggregationQuery query = new AggregationQuery()
				.keys(InvertedIndexRecord.KEYS)
				.fields(InvertedIndexQueryResult.OUTPUT_FIELDS);
		StreamConsumers.ToList<InvertedIndexQueryResult> consumerToList = StreamConsumers.toList(eventloop);
		aggregation.query(0, query, InvertedIndexQueryResult.class).streamTo(consumerToList);
		eventloop.run();

		System.out.println("Query result:");
		System.out.println(consumerToList.getList());

		executorService.shutdown();
	}
}
