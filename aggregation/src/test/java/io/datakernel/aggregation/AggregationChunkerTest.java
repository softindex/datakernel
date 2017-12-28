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

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.async.Stages;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class AggregationChunkerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException, ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = new AggregationStructure()
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationState state = new AggregationState(structure);

		final List<StreamConsumer> listConsumers = new ArrayList<>();

		final List items = new ArrayList();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			long chunkId;

			@Override
			public <T> CompletionStage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				StreamProducer streamProducer = StreamProducers.ofIterator(eventloop, items.iterator());
				return Stages.of(StreamProducers.withEndOfStream(streamProducer));
			}

			@Override
			public <T> CompletionStage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = new StreamConsumerToList<>(eventloop, items);
				listConsumers.add(consumer);
				return Stages.of(StreamConsumers.withEndOfStream(consumer));
			}

			@Override
			public CompletionStage<Long> createId() {
				return Stages.of(++chunkId);
			}

			@Override
			public CompletionStage<Void> finish(Set<Long> chunkIds) {
				return Stages.of(null);
			}
		};

		final List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamProducer<KeyValuePair> producer = StreamProducers.ofIterable(eventloop, asList(
				new KeyValuePair(3, 4, 6),
				new KeyValuePair(3, 6, 7),
				new KeyValuePair(1, 2, 1)));
		AggregationChunker<KeyValuePair> chunker = AggregationChunker.create(eventloop,
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader).withChunkSize(1);
		producer.streamTo(chunker);

		CompletableFuture<List<AggregationChunk>> future = chunker.getResult().toCompletableFuture();

		eventloop.run();

		assertEquals(3, future.get().size());

		assertEquals(new KeyValuePair(3, 4, 6), items.get(0));
		assertEquals(new KeyValuePair(3, 6, 7), items.get(1));
		assertEquals(new KeyValuePair(1, 2, 1), items.get(2));

		assertStatus(StreamStatus.END_OF_STREAM, producer);
//		assertStatus(StreamStatus.END_OF_STREAM, chunker);
//		assertStatus(((StreamProducers.OfIterator) producer).getDownstream(), chunker);
		for (StreamConsumer consumer : listConsumers) {
			assertStatus(StreamStatus.END_OF_STREAM, consumer);
		}
	}

	@Test
	public void testProducerWithError() throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = new AggregationStructure()
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationState state = new AggregationState(structure);
		final List<StreamConsumer> listConsumers = new ArrayList<>();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			long chunkId;
			final List items = new ArrayList();

			@Override
			public <T> CompletionStage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				return Stages.of(StreamProducers.withEndOfStream(StreamProducers.ofIterator(eventloop, items.iterator())));
			}

			@Override
			public <T> CompletionStage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = new StreamConsumerToList<>(eventloop, items);
				listConsumers.add(consumer);
				return Stages.of(StreamConsumers.withEndOfStream(consumer));
			}

			@Override
			public CompletionStage<Long> createId() {
				return Stages.of(++chunkId);
			}

			@Override
			public CompletionStage<Void> finish(Set<Long> chunkIds) {
				return Stages.of(null);
			}
		};

		final List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamProducer<KeyValuePair> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2))),
				StreamProducers.<KeyValuePair>closingWithError(new ExpectedException("Test Exception"))
		);
		AggregationChunker<KeyValuePair> chunker = AggregationChunker.create(eventloop,
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader).withChunkSize(1);
		producer.streamTo(chunker);

		CompletableFuture<List<AggregationChunk>> future = chunker.getResult().toCompletableFuture();

		eventloop.run();

		assertTrue(future.isCompletedExceptionally());
		assertStatus(StreamStatus.CLOSED_WITH_ERROR, producer);
//		assertStatus(StreamStatus.CLOSED_WITH_ERROR, chunker);
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertStatus(StreamStatus.END_OF_STREAM, listConsumers.get(i));
		}
		assertStatus(StreamStatus.CLOSED_WITH_ERROR, getLast(listConsumers));
	}

	private static <T> T getLast(List<T> list) {
		return list.get(list.size() - 1);
	}

	@Test
	public void testStorageConsumerWithError() throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = new AggregationStructure()
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationState metadata = new AggregationState(structure);
		final List<StreamConsumer> listConsumers = new ArrayList<>();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			long chunkId;
			final List items = new ArrayList();

			@Override
			public <T> CompletionStage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				return Stages.of(StreamProducers.withEndOfStream(StreamProducers.ofIterator(eventloop, items.iterator())));
			}

			@Override
			public <T> CompletionStage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				if (chunkId == 1) {
					StreamConsumerToList<T> toList = new StreamConsumerToList<>(eventloop, items);
					listConsumers.add(toList);
					return Stages.of(StreamConsumers.withEndOfStream(toList));
				} else {
					StreamConsumer<T> consumer = StreamConsumers.closingWithError(new Exception("Test Exception"));
					listConsumers.add(consumer);
					return Stages.of(StreamConsumers.withEndOfStream(consumer));
				}
			}

			@Override
			public CompletionStage<Long> createId() {
				return Stages.of(++chunkId);
			}

			@Override
			public CompletionStage<Void> finish(Set<Long> chunkIds) {
				return Stages.of(null);
			}
		};

		final List<AggregationChunk> chunksToConsolidate = metadata.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamProducer<KeyValuePair> producer = StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2), new KeyValuePair(1, 1, 2), new KeyValuePair(1, 1, 2))
		);
		AggregationChunker<KeyValuePair> chunker = AggregationChunker.create(eventloop,
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader).withChunkSize(1);
		producer.streamTo(chunker);

		CompletableFuture<List<AggregationChunk>> future = chunker.getResult().toCompletableFuture();
		eventloop.run();

		assertTrue(future.isCompletedExceptionally());
		assertStatus(StreamStatus.CLOSED_WITH_ERROR, producer);
//		assertStatus(StreamStatus.CLOSED_WITH_ERROR, chunker);
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertStatus(StreamStatus.END_OF_STREAM, listConsumers.get(i));
		}
		assertStatus(StreamStatus.CLOSED_WITH_ERROR, getLast(listConsumers));
	}
}
