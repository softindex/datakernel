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

package io.datakernel.aggregation;

import io.datakernel.aggregation.util.BiPredicate;
import io.datakernel.aggregation.util.Predicates;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.aggregation.AggregationGroupReducerTest.NO_OP_TRACKER;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationChunkerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub(eventloop);
		Aggregation structure = Aggregation.createUninitialized()
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationMetadata metadata = new AggregationMetadata(structure);
		final List<StreamConsumer> listConsumers = new ArrayList<>();

		final List items = new ArrayList();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			@Override
			public <T> StreamProducer<T> chunkReader(Aggregation structure, List<String> keys, List<String> fields, Class<T> recordClass, long id, DefiningClassLoader classLoader) {
				return new StreamProducers.OfIterator<>(eventloop, items.iterator());
			}

			@Override
			public <T> void chunkWriter(Aggregation structure, List<String> keys, List<String> fields, Class<T> recordClass, long id, StreamProducer<T> producer, DefiningClassLoader classLoader, CompletionCallback callback) {
				StreamConsumers.ToList<T> consumer = StreamConsumers.toList(eventloop, items);
				consumer.setCompletionCallback(callback);
				listConsumers.add(consumer);
				producer.streamTo(consumer);
			}

			@Override
			public void removeChunk(long id, CompletionCallback callback) {
			}
		};

		final List<List<AggregationChunk.NewChunk>> list = new ArrayList<>();
		ResultCallback<List<AggregationChunk.NewChunk>> resultCallback = new ResultCallback<List<AggregationChunk.NewChunk>>() {
			@Override
			protected void onException(Exception exception) {
			}

			@Override
			protected void onResult(List<AggregationChunk.NewChunk> result) {
				list.add(result);
			}
		};

		final List<AggregationChunk> chunksToConsolidate = metadata.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getFields()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		AggregationChunker aggregationChunker = new AggregationChunker<>(eventloop, NO_OP_TRACKER,
				structure, structure.getKeys(), structure.getFields(), recordClass, (BiPredicate) Predicates.alwaysTrue(),
				aggregationChunkStorage, aggregationMetadataStorage, 1, classLoader, resultCallback);

		StreamProducer<KeyValuePair> producer = StreamProducers.ofIterable(eventloop,
				asList(new KeyValuePair(3, 4, 6), new KeyValuePair(3, 6, 7), new KeyValuePair(1, 2, 1)));
		producer.streamTo(aggregationChunker);

		eventloop.run();

		assertTrue(list.size() == 1);
		assertTrue(list.get(0).size() == 3);

		assertEquals(new KeyValuePair(3, 4, 6), items.get(0));
		assertEquals(new KeyValuePair(3, 6, 7), items.get(1));
		assertEquals(new KeyValuePair(1, 2, 1), items.get(2));

		assertEquals(StreamStatus.END_OF_STREAM, producer.getProducerStatus());
		assertEquals(StreamStatus.END_OF_STREAM, aggregationChunker.getConsumerStatus());
		assertEquals(((StreamProducers.OfIterator) producer).getDownstream(), aggregationChunker);
		for (StreamConsumer consumer : listConsumers) {
			assertEquals(consumer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		}
	}

	@Test
	public void testProducerWithError() throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub(eventloop);
		Aggregation structure = Aggregation.createUninitialized()
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationMetadata metadata = new AggregationMetadata(structure);
		final List<StreamConsumer> listConsumers = new ArrayList<>();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			final List items = new ArrayList();

			@Override
			public <T> StreamProducer<T> chunkReader(Aggregation structure, List<String> keys, List<String> fields, Class<T> recordClass, long id, DefiningClassLoader classLoader) {
				return new StreamProducers.OfIterator<>(eventloop, items.iterator());
			}

			@Override
			public <T> void chunkWriter(Aggregation structure, List<String> keys, List<String> fields, Class<T> recordClass, long id, StreamProducer<T> producer, DefiningClassLoader classLoader, CompletionCallback callback) {
				StreamConsumers.ToList<T> consumer = StreamConsumers.toList(eventloop, items);
				consumer.setCompletionCallback(callback);
				listConsumers.add(consumer);
				producer.streamTo(consumer);
			}

			@Override
			public void removeChunk(long id, CompletionCallback callback) {

			}
		};

		List<String> keys = structure.getKeys();

		final List<List<AggregationChunk.NewChunk>> list = new ArrayList<>();
		ResultCallback<List<AggregationChunk.NewChunk>> resultCallback = new ResultCallback<List<AggregationChunk.NewChunk>>() {

			@Override
			protected void onException(Exception exception) {
			}

			@Override
			protected void onResult(List<AggregationChunk.NewChunk> result) {
				list.add(result);
			}
		};

		final List<AggregationChunk> chunksToConsolidate = metadata.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getFields()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, keys, fields, classLoader);

		AggregationChunker aggregationChunker = new AggregationChunker<>(eventloop, NO_OP_TRACKER,
				structure, structure.getKeys(), structure.getFields(), recordClass, (BiPredicate) Predicates.alwaysTrue(),
				aggregationChunkStorage, aggregationMetadataStorage, 1, classLoader, resultCallback);

		StreamProducer<KeyValuePair> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.<KeyValuePair>closingWithError(eventloop, new Exception("Test Exception"))
		);

		producer.streamTo(aggregationChunker);

		eventloop.run();

		assertTrue(list.size() == 0);
		assertEquals(StreamStatus.CLOSED_WITH_ERROR, producer.getProducerStatus());
		assertEquals(StreamStatus.CLOSED_WITH_ERROR, aggregationChunker.getConsumerStatus());
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEquals(listConsumers.get(i).getConsumerStatus(), StreamStatus.END_OF_STREAM);
		}
		assertEquals(getLast(listConsumers).getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);
	}

	static <T> T getLast(List<T> list) {
		return list.get(list.size() - 1);
	}

	@Test
	public void testStorageConsumerWithError() throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub(eventloop);
		Aggregation structure = Aggregation.createUninitialized()
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationMetadata metadata = new AggregationMetadata(structure);
		final List<StreamConsumer> listConsumers = new ArrayList<>();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			final List items = new ArrayList();

			@Override
			public <T> StreamProducer<T> chunkReader(Aggregation aggregation, List<String> keys, List<String> fields, Class<T> recordClass, long id, DefiningClassLoader classLoader) {
				return new StreamProducers.OfIterator<>(eventloop, items.iterator());
			}

			@Override
			public <T> void chunkWriter(Aggregation aggregation, List<String> keys, List<String> fields, Class<T> recordClass, long id, StreamProducer<T> producer, DefiningClassLoader classLoader, CompletionCallback callback) {
				if (id == 1) {
					StreamConsumers.ToList<T> toList = StreamConsumers.toList(eventloop, items);
					listConsumers.add(toList);
					producer.streamTo(toList);
				} else {
					StreamConsumers.ClosingWithError<T> consumer = StreamConsumers.closingWithError(eventloop, new Exception("Test Exception"));
					consumer.setCompletionCallback(callback);
					listConsumers.add(consumer);
					producer.streamTo(consumer);
				}
			}

			@Override
			public void removeChunk(long id, CompletionCallback callback) {

			}
		};

		final List<List<AggregationChunk.NewChunk>> list = new ArrayList<>();
		ResultCallback<List<AggregationChunk.NewChunk>> resultCallback = new ResultCallback<List<AggregationChunk.NewChunk>>() {

			@Override
			protected void onException(Exception exception) {
			}

			@Override
			protected void onResult(List<AggregationChunk.NewChunk> result) {
				list.add(result);
			}
		};

		final List<AggregationChunk> chunksToConsolidate = metadata.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getFields()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		AggregationChunker aggregationChunker = new AggregationChunker<>(eventloop, NO_OP_TRACKER,
				structure, structure.getKeys(), structure.getFields(), recordClass, (BiPredicate) Predicates.alwaysTrue(),
				aggregationChunkStorage, aggregationMetadataStorage, 1, classLoader, resultCallback);

		StreamProducer<KeyValuePair> producer = StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2), new KeyValuePair(1, 1, 2), new KeyValuePair(1, 1, 2))
		);
		producer.streamTo(aggregationChunker);

		eventloop.run();

		assertTrue(list.size() == 0);
		assertEquals(StreamStatus.CLOSED_WITH_ERROR, producer.getProducerStatus());
		assertEquals(((StreamProducers.OfIterator) producer).getDownstream(), aggregationChunker);
		assertEquals(StreamStatus.CLOSED_WITH_ERROR, aggregationChunker.getConsumerStatus());
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEquals(listConsumers.get(i).getConsumerStatus(), StreamStatus.END_OF_STREAM);
		}
		assertEquals(getLast(listConsumers).getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);
	}
}
