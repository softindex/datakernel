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
import io.datakernel.async.Stage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestUtils.assertStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class AggregationChunkerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = AggregationStructure.create(ChunkIdScheme.ofLong())
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationState state = new AggregationState(structure);

		List<StreamConsumer> listConsumers = new ArrayList<>();

		List items = new ArrayList();
		AggregationChunkStorage<Long> aggregationChunkStorage = new AggregationChunkStorage<Long>() {
			long chunkId;

			@Override
			public <T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Stage.of(StreamProducer.ofIterable(items).withEndOfStreamAsResult());
			}

			@Override
			public <T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create(items);
				listConsumers.add(consumer);
				return Stage.of(consumer.withEndOfStreamAsResult());
			}

			@Override
			public Stage<Long> createId() {
				return Stage.of(++chunkId);
			}

			@Override
			public Stage<Void> finish(Set<Long> chunkIds) {
				return Stage.of(null);
			}
		};

		List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamProducer<KeyValuePair> producer = StreamProducer.of(
				new KeyValuePair(3, 4, 6),
				new KeyValuePair(3, 6, 7),
				new KeyValuePair(1, 2, 1));
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		CompletableFuture<List<AggregationChunk>> future = producer.streamTo(chunker).getConsumerResult().toCompletableFuture();

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
	public void testProducerWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = AggregationStructure.create(ChunkIdScheme.ofLong())
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationState state = new AggregationState(structure);
		List<StreamConsumer> listConsumers = new ArrayList<>();
		AggregationChunkStorage<Long> aggregationChunkStorage = new AggregationChunkStorage<Long>() {
			long chunkId;
			final List items = new ArrayList();

			@Override
			public <T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Stage.of(StreamProducer.ofIterable(items).withEndOfStreamAsResult());
			}

			@Override
			public <T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create(items);
				listConsumers.add(consumer);
				return Stage.of(consumer.withEndOfStreamAsResult());
			}

			@Override
			public Stage<Long> createId() {
				return Stage.of(++chunkId);
			}

			@Override
			public Stage<Void> finish(Set<Long> chunkIds) {
				return Stage.of(null);
			}
		};

		List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamProducer<KeyValuePair> producer = StreamProducer.concat(
				StreamProducer.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamProducer.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamProducer.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamProducer.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamProducer.closingWithError(new ExpectedException("Test Exception"))
		);
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		CompletableFuture<List<AggregationChunk>> future = producer.streamTo(chunker).getConsumerResult().toCompletableFuture();

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
	public void testStorageConsumerWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = AggregationStructure.create(ChunkIdScheme.ofLong())
				.withKey("key", ofInt())
				.withMeasure("value", sum(ofInt()))
				.withMeasure("timestamp", sum(ofLong()));
		AggregationState metadata = new AggregationState(structure);
		List<StreamConsumer> listConsumers = new ArrayList<>();
		AggregationChunkStorage<Long> aggregationChunkStorage = new AggregationChunkStorage<Long>() {
			long chunkId;
			final List items = new ArrayList();

			@Override
			public <T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Stage.of(StreamProducer.ofIterable(items).withEndOfStreamAsResult());
			}

			@Override
			public <T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				if (chunkId == 1) {
					StreamConsumerToList<T> toList = StreamConsumerToList.create(items);
					listConsumers.add(toList);
					return Stage.of(toList.withEndOfStreamAsResult());
				} else {
					StreamConsumer<T> consumer = StreamConsumer.closingWithError(new Exception("Test Exception"));
					listConsumers.add(consumer);
					return Stage.of(consumer.withEndOfStreamAsResult());
				}
			}

			@Override
			public Stage<Long> createId() {
				return Stage.of(++chunkId);
			}

			@Override
			public Stage<Void> finish(Set<Long> chunkIds) {
				return Stage.of(null);
			}
		};

		List<AggregationChunk> chunksToConsolidate = metadata.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = AggregationUtils.createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamProducer<KeyValuePair> producer = StreamProducer.of(
				new KeyValuePair(1, 1, 0),
				new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2));
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		CompletableFuture<List<AggregationChunk>> future = producer.streamTo(chunker).getConsumerResult().toCompletableFuture();
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
