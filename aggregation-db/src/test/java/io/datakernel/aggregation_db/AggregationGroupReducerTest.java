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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.processor.ProcessorFactory;
import io.datakernel.aggregation_db.util.Predicates;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.aggregation_db.AggregationStructure.createKeyFunction;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.intList;
import static io.datakernel.aggregation_db.keytype.KeyTypes.stringKey;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings({"Duplicates", "unchecked"})
public class AggregationGroupReducerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static AggregationOperationTracker NO_OP_TRACKER = new AggregationOperationTracker() {
		@Override
		public void reportStart(AggregationChunker chunker) { }

		@Override
		public void reportCompletion(AggregationChunker chunker) { }

		@Override
		public void reportStart(AggregationGroupReducer groupReducer) { }

		@Override
		public void reportCompletion(AggregationGroupReducer groupReducer) { }
	};

	@Test
	public void test() throws IOException {
		final Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationMetadata aggregationMetadata = new AggregationMetadata(InvertedIndexRecord.KEYS,
				InvertedIndexRecord.OUTPUT_FIELDS);
		AggregationStructure structure = new AggregationStructure(
				ImmutableMap.<String, KeyType>builder()
						.put("word", stringKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("documents", intList())
						.build());
		ProcessorFactory processorFactory = new ProcessorFactory(structure);

		final List<StreamConsumer> listConsumers = new ArrayList<>();
		final List items = new ArrayList();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {

			@Override
			public <T> StreamProducer<T> chunkReader(List<String> keys, List<String> fields, Class<T> recordClass,
			                                         long id, DefiningClassLoader classLoader) {
				return new StreamProducers.OfIterator<T>(eventloop, items.iterator());
			}

			@Override
			public <T> void chunkWriter(List<String> keys, List<String> fields, Class<T> recordClass, long id,
			                            StreamProducer<T> producer, DefiningClassLoader classLoader,
			                            CompletionCallback callback) {
				StreamConsumers.ToList consumer = StreamConsumers.toList(eventloop, items);
				consumer.setCompletionCallback(callback);
				listConsumers.add(consumer);
				producer.streamTo(consumer);
			}

			@Override
			public void removeChunk(long id, CompletionCallback callback) {

			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<?> keyClass = structure.createKeyClass(aggregationMetadata.getKeys(), classLoader);
		Class<?> aggregationClass = structure.createRecordClass(aggregationMetadata.getKeys(),
				aggregationMetadata.getFields(), classLoader);

		Function<InvertedIndexRecord, Comparable<?>> keyFunction = createKeyFunction(inputClass, keyClass,
				aggregationMetadata.getKeys(), classLoader);

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, aggregationMetadata.getKeys(),
				InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS, classLoader);

		int aggregationChunkSize = 2;
		final List<List<AggregationChunk.NewChunk>> listCallback = new ArrayList<>();
		ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback = new ResultCallback<List<AggregationChunk.NewChunk>>() {
			@Override
			public void onResult(List<AggregationChunk.NewChunk> result) {
				listCallback.add(result);
			}

			@Override
			public void onException(Exception exception) {
				fail(exception.getMessage());
			}
		};

		AggregationGroupReducer<InvertedIndexRecord> aggregationGroupReducer = new AggregationGroupReducer<>(eventloop,
				aggregationChunkStorage, NO_OP_TRACKER, aggregationMetadataStorage, aggregationMetadata.getKeys(),
				aggregationMetadata.getFields(), aggregationClass,
				Predicates.<InvertedIndexRecord, InvertedIndexRecord>alwaysTrue(), keyFunction, aggregate, aggregationChunkSize, classLoader, chunksCallback);

		StreamProducer<InvertedIndexRecord> producer = StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3),
				new InvertedIndexRecord("brown", 3), new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1), new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10)));

		producer.streamTo(aggregationGroupReducer);

		eventloop.run();

		assertEquals(producer.getProducerStatus(), StreamStatus.END_OF_STREAM);
		assertEquals(aggregationGroupReducer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		assertEquals(listCallback.size(), 1);

		assertEquals((listCallback.get(0)).size(), 5);
		for (StreamConsumer consumer : listConsumers) {
			assertEquals(consumer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		}
	}

	@Test
	public void testProducerWithError() throws IOException {
		final Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationMetadata aggregationMetadata = new AggregationMetadata(InvertedIndexRecord.KEYS,
				InvertedIndexRecord.OUTPUT_FIELDS);
		AggregationStructure structure = new AggregationStructure(
				ImmutableMap.<String, KeyType>builder()
						.put("word", stringKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("documents", intList())
						.build());
		ProcessorFactory processorFactory = new ProcessorFactory(structure);

		final List<StreamConsumer> listConsumers = new ArrayList<>();
		final List items = new ArrayList();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {

			@Override
			public <T> StreamProducer<T> chunkReader(List<String> keys, List<String> fields, Class<T> recordClass,
			                                         long id, DefiningClassLoader classLoader) {
				return new StreamProducers.OfIterator<T>(eventloop, items.iterator());
			}

			@Override
			public <T> void chunkWriter(List<String> keys, List<String> fields, Class<T> recordClass, long id,
			                            StreamProducer<T> producer, DefiningClassLoader classLoader,
			                            CompletionCallback callback) {
				StreamConsumers.ToList consumer = StreamConsumers.toList(eventloop, items);
				consumer.setCompletionCallback(callback);
				listConsumers.add(consumer);
				producer.streamTo(consumer);
			}

			@Override
			public void removeChunk(long id, CompletionCallback callback) {

			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<?> keyClass = structure.createKeyClass(aggregationMetadata.getKeys(), classLoader);
		Class<?> aggregationClass = structure.createRecordClass(aggregationMetadata.getKeys(),
				aggregationMetadata.getFields(), classLoader);

		Function<InvertedIndexRecord, Comparable<?>> keyFunction = createKeyFunction(inputClass, keyClass,
				aggregationMetadata.getKeys(), classLoader);

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, aggregationMetadata.getKeys(),
				InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS, classLoader);

		int aggregationChunkSize = 2;
		final List<List<AggregationChunk.NewChunk>> listCallback = new ArrayList<>();
		ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback = new ResultCallback<List<AggregationChunk.NewChunk>>() {
			@Override
			public void onResult(List<AggregationChunk.NewChunk> result) {
				listCallback.add(result);
			}

			@Override
			public void onException(Exception exception) {
				fail(exception.getMessage());
			}
		};

		AggregationGroupReducer<InvertedIndexRecord> aggregationGroupReducer = new AggregationGroupReducer<>(eventloop,
				aggregationChunkStorage, NO_OP_TRACKER, aggregationMetadataStorage, aggregationMetadata.getKeys(),
				aggregationMetadata.getFields(), aggregationClass, Predicates.<InvertedIndexRecord, InvertedIndexRecord>alwaysTrue(),
				keyFunction, aggregate, aggregationChunkSize, classLoader, chunksCallback);

		StreamProducer<InvertedIndexRecord> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("fox", 1),
						new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3),
						new InvertedIndexRecord("brown", 3), new InvertedIndexRecord("lazy", 4),
						new InvertedIndexRecord("dog", 1), new InvertedIndexRecord("quick", 1),
						new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10),
						new InvertedIndexRecord("brown", 12))),
				StreamProducers.<InvertedIndexRecord>closingWithError(eventloop, new Exception("Test Exception")));

		producer.streamTo(aggregationGroupReducer);

		eventloop.run();

		assertEquals(producer.getProducerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(aggregationGroupReducer.getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(listCallback.size(), 0);
		for (StreamConsumer consumer : listConsumers) {
			assertEquals(consumer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		}
	}
}
