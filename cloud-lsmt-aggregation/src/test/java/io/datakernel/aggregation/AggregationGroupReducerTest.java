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

import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.aggregation.StreamUtils.assertEndOfStream;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.measure.Measures.union;
import static io.datakernel.aggregation.util.Utils.*;
import static io.datakernel.common.collection.CollectionUtils.keysToMap;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"Duplicates", "unchecked", "ArraysAsListWithZeroOrOneArgument", "rawtypes"})
public class AggregationGroupReducerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withKey("word", FieldTypes.ofString())
				.withMeasure("documents", union(ofInt()));

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
				StreamConsumerToList consumer = StreamConsumerToList.create(items);
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

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<Comparable> keyClass = createKeyClass(
				keysToMap(Stream.of("word"), structure.getKeyTypes()::get),
				classLoader);
		Class<InvertedIndexRecord> aggregationClass = createRecordClass(structure, asList("word"), asList("documents"), classLoader);

		Function<InvertedIndexRecord, Comparable> keyFunction = createKeyFunction(inputClass, keyClass,
				asList("word"), classLoader);

		Aggregate<InvertedIndexRecord, Object> aggregate = createPreaggregator(structure, inputClass, aggregationClass,
				singletonMap("word", "word"), singletonMap("documents", "documentId"), classLoader);

		int aggregationChunkSize = 2;

		StreamSupplier<InvertedIndexRecord> supplier = StreamSupplier.of(
				new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2),
				new InvertedIndexRecord("fox", 3),
				new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1),
				new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4),
				new InvertedIndexRecord("brown", 10));

		AggregationGroupReducer<Long, InvertedIndexRecord, Comparable> groupReducer = new AggregationGroupReducer<>(aggregationChunkStorage,
				structure, asList("documents"),
				aggregationClass, singlePartition(), keyFunction, aggregate, aggregationChunkSize, classLoader);

		await(supplier.streamTo(groupReducer));
		List<AggregationChunk> list = await(groupReducer.getResult());

		assertEndOfStream(supplier);
		assertEndOfStream(groupReducer);
		assertEquals(5, list.size());

		for (StreamConsumer consumer : listConsumers) {
			assertEndOfStream(consumer);
		}
	}

}
