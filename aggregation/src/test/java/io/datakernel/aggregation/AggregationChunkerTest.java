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
import io.datakernel.async.Promise;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
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
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"unchecked", "rawtypes"})
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
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create(items);
				listConsumers.add(consumer);
				return Promise.of(consumer);
			}

			@Override
			public Promise<Long> createId() {
				return Promise.of(++chunkId);
			}

			@Override
			public Promise<Void> finish(Set<Long> chunkIds) {
				return Promise.complete();
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

		StreamSupplier<KeyValuePair> supplier = StreamSupplier.of(
				new KeyValuePair(3, 4, 6),
				new KeyValuePair(3, 6, 7),
				new KeyValuePair(1, 2, 1)
		);
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		CompletableFuture<List<AggregationChunk>> future = supplier.streamTo(chunker)
				.thenCompose($ -> chunker.getResult())
				.toCompletableFuture();

		eventloop.run();

		assertEquals(3, future.get().size());

		assertEquals(new KeyValuePair(3, 4, 6), items.get(0));
		assertEquals(new KeyValuePair(3, 6, 7), items.get(1));
		assertEquals(new KeyValuePair(1, 2, 1), items.get(2));

		assertEndOfStream(supplier);
//		assertEndOfStream(chunker);
//		assertStatus(((StreamSuppliers.OfIterator) supplier).getDownstream(), chunker);
		for (StreamConsumer consumer : listConsumers) {
			assertEndOfStream(consumer);
		}
	}

	@Test
	public void testSupplierWithError() {
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
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create(items);
				listConsumers.add(consumer);
				return Promise.of(consumer);
			}

			@Override
			public Promise<Long> createId() {
				return Promise.of(++chunkId);
			}

			@Override
			public Promise<Void> finish(Set<Long> chunkIds) {
				return Promise.complete();
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

		StreamSupplier<KeyValuePair> supplier = StreamSupplier.concat(
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.closingWithError(new ExpectedException("Test Exception"))
		);
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		CompletableFuture<List<AggregationChunk>> future = supplier.streamTo(chunker)
				.thenCompose($ -> chunker.getResult())
				.toCompletableFuture();

		eventloop.run();

		assertTrue(future.isCompletedExceptionally());
		assertClosedWithError(supplier);
//		assertClosedWithError(chunker);
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEndOfStream(listConsumers.get(i));
		}
		assertClosedWithError(getLast(listConsumers));
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
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				if (chunkId == 1) {
					StreamConsumerToList<T> toList = StreamConsumerToList.create(items);
					listConsumers.add(toList);
					return Promise.of(toList);
				} else {
					StreamConsumer<T> consumer = StreamConsumer.closingWithError(new Exception("Test Exception"));
					listConsumers.add(consumer);
					return Promise.of(consumer);
				}
			}

			@Override
			public Promise<Long> createId() {
				return Promise.of(++chunkId);
			}

			@Override
			public Promise<Void> finish(Set<Long> chunkIds) {
				return Promise.complete();
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

		StreamSupplier<KeyValuePair> supplier = StreamSupplier.of(
				new KeyValuePair(1, 1, 0),
				new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2));
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		CompletableFuture<List<AggregationChunk>> future = supplier.streamTo(chunker)
				.thenCompose($ -> chunker.getResult())
				.toCompletableFuture();

		eventloop.run();

		assertTrue(future.isCompletedExceptionally());
		assertClosedWithError(supplier);
//		assertClosedWithError(chunker);
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEndOfStream(listConsumers.get(i));
		}
		assertClosedWithError(getLast(listConsumers));
	}
}
