/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static io.datakernel.test.TestUtils.assertComplete;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(DatakernelRunner.class)
public final class AggregationChunkerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private final DefiningClassLoader classLoader = DefiningClassLoader.create();
	private final AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
			.withKey("key", ofInt())
			.withMeasure("value", sum(ofInt()))
			.withMeasure("timestamp", sum(ofLong()));
	private final AggregationState state = new AggregationState(structure);

	@Test
	public void test() {
		List<KeyValuePair> items = new ArrayList<>();
		AggregationChunkStorage<Long> aggregationChunkStorage = new AggregationChunkStorage<Long>() {
			long chunkId;

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable((Iterable<T>) items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create((List<T>) items);
				consumer.getEndOfStream().acceptEx(assertComplete());
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

		AggregationChunker<?, KeyValuePair> chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		StreamSupplier<KeyValuePair> supplier = StreamSupplier.of(
				new KeyValuePair(3, 4, 6),
				new KeyValuePair(3, 6, 7),
				new KeyValuePair(1, 2, 1)
		);

		await(supplier.streamTo(chunker));
		List<AggregationChunk> list = await(chunker.getResult());

		assertEquals(3, list.size());
		assertEquals(new KeyValuePair(3, 4, 6), items.get(0));
		assertEquals(new KeyValuePair(3, 6, 7), items.get(1));
		assertEquals(new KeyValuePair(1, 2, 1), items.get(2));
	}

	@Test
	public void testSupplierWithError() {
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

		ExpectedException exception = new ExpectedException("Test Exception");
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
				StreamSupplier.closingWithError(exception)
		);
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		Throwable e = awaitException(supplier.streamTo(chunker));
		assertSame(exception, e);

		// this must be checked after eventloop completion :(
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEndOfStream(listConsumers.get(i));
		}
		assertClosedWithError(listConsumers.get(listConsumers.size() - 1));
	}

	@Test
	public void testStorageConsumerWithError() {
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
				new KeyValuePair(1, 1, 0),
				new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2));
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) AggregationUtils.singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		awaitException(supplier.streamTo(chunker));

		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEndOfStream(listConsumers.get(i));
		}
		assertClosedWithError(listConsumers.get(listConsumers.size() - 1));
	}
}
