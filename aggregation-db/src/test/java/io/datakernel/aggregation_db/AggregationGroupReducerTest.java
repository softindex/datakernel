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
import io.datakernel.aggregation_db.fieldtype.FieldTypeInt;
import io.datakernel.aggregation_db.fieldtype.FieldTypeList;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeString;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.aggregation_db.InvertedIndexTest.InvertedIndexRecord;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AggregationGroupReducerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		final Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationMetadata aggregationMetadata = new AggregationMetadata("inverted index", InvertedIndexRecord.KEYS,
				InvertedIndexRecord.INPUT_FIELDS, InvertedIndexRecord.OUTPUT_FIELDS);
		ProcessorFactory processorFactory = new InvertedIndexProcessorFactory(classLoader);
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

		final List<StreamConsumer> listConsumers = new ArrayList<>();
		final Map<Long, List> map = new HashMap<>();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {

			@Override
			public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id) {
				return new StreamProducers.OfIterator<T>(eventloop, map.get(aggregationId).iterator());
			}

			@Override
			public <T> void chunkWriter(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id, StreamProducer<T> producer, CompletionCallback callback) {
				List<T> list = new ArrayList<>();
				map.put(id, list);
				StreamConsumers.ToList consumer = StreamConsumers.toList(eventloop, list);
				consumer.setCompletionCallback(callback);
				listConsumers.add(consumer);
				producer.streamTo(consumer);
			}

			@Override
			public void removeChunk(String aggregationId, long id, CompletionCallback callback) {

			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		List<String> inputFields = aggregationMetadata.getInputFields();
		Class<?> keyClass = structure.createKeyClass(aggregationMetadata.getKeys());
		Class<?> aggregationClass = structure.createRecordClass(aggregationMetadata.getKeys(), aggregationMetadata.getOutputFields());

		Function<InvertedIndexRecord, Comparable<?>> keyFunction = structure.createKeyFunction(inputClass, keyClass,
				aggregationMetadata.getKeys());

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, aggregationMetadata.getKeys(),
				inputFields, aggregationMetadata.getOutputFields());

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
				aggregationChunkStorage, aggregationMetadataStorage, aggregationMetadata, null, aggregationClass,
				keyFunction, aggregate, chunksCallback, aggregationChunkSize);

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
		AggregationMetadata aggregationMetadata = new AggregationMetadata("inverted index", InvertedIndexRecord.KEYS,
				InvertedIndexRecord.INPUT_FIELDS, InvertedIndexRecord.OUTPUT_FIELDS);
		ProcessorFactory processorFactory = new InvertedIndexProcessorFactory(classLoader);
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

		final List<StreamConsumer> listConsumers = new ArrayList<>();
		final Map<String, List> map = new HashMap<>();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {

			@Override
			public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id) {
				return new StreamProducers.OfIterator<T>(eventloop, map.get(aggregationId).iterator());
			}

			@Override
			public <T> void chunkWriter(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id, StreamProducer<T> producer, CompletionCallback callback) {
				List<T> list = new ArrayList<>();
				map.put(aggregationId, list);
				StreamConsumers.ToList consumer = StreamConsumers.toList(eventloop, list);
				consumer.setCompletionCallback(callback);
				listConsumers.add(consumer);
				producer.streamTo(consumer);
			}

			@Override
			public void removeChunk(String aggregationId, long id, CompletionCallback callback) {

			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		List<String> inputFields = aggregationMetadata.getInputFields();
		Class<?> keyClass = structure.createKeyClass(aggregationMetadata.getKeys());
		Class<?> aggregationClass = structure.createRecordClass(aggregationMetadata.getKeys(), aggregationMetadata.getOutputFields());

		Function<InvertedIndexRecord, Comparable<?>> keyFunction = structure.createKeyFunction(inputClass, keyClass,
				aggregationMetadata.getKeys());

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, aggregationMetadata.getKeys(),
				inputFields, aggregationMetadata.getOutputFields());

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
				aggregationChunkStorage, aggregationMetadataStorage, aggregationMetadata, null, aggregationClass,
				keyFunction, aggregate, chunksCallback, aggregationChunkSize);

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
