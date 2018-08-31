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
import io.datakernel.async.Stage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static io.datakernel.aggregation.AggregationUtils.*;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.measure.Measures.union;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestUtils.assertStatus;
import static io.datakernel.util.CollectionUtils.keysToMap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"Duplicates", "unchecked", "ArraysAsListWithZeroOrOneArgument"})
public class AggregationGroupReducerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = AggregationStructure.create(ChunkIdScheme.ofLong())
				.withKey("word", FieldTypes.ofString())
				.withMeasure("documents", union(ofInt()));

		List<StreamConsumer> listConsumers = new ArrayList<>();
		List items = new ArrayList();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage<Long>() {
			long chunkId;

			@Override
			public <T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Stage.of(StreamProducer.ofIterable(items).withEndOfStreamAsResult());
			}

			@Override
			public <T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList consumer = StreamConsumerToList.create(items);
				listConsumers.add(consumer);
				return Stage.of(consumer.withEndOfStreamAsResult());
			}

			@Override
			public Stage<Long> createId() {
				return Stage.of(++chunkId);
			}

			@Override
			public Stage<Void> finish(Set<Long> chunkIds) {
				return Stage.complete();
			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<?> keyClass = createKeyClass(
				keysToMap(asList("word").stream(), structure.getKeyTypes()::get),
				classLoader);
		Class<?> aggregationClass = createRecordClass(structure, asList("word"), asList("documents"), classLoader);

		Function<InvertedIndexRecord, Comparable<?>> keyFunction = createKeyFunction(inputClass, keyClass,
				asList("word"), classLoader);

		Aggregate aggregate = createPreaggregator(structure, inputClass, aggregationClass,
				singletonMap("word", "word"), singletonMap("documents", "documentId"), classLoader);

		int aggregationChunkSize = 2;

		StreamProducer<InvertedIndexRecord> producer = StreamProducer.of(new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3),
				new InvertedIndexRecord("brown", 3), new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1), new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10));

		AggregationGroupReducer groupReducer = new AggregationGroupReducer(aggregationChunkStorage,
				structure, asList("documents"),
				aggregationClass, singlePartition(), keyFunction, aggregate, aggregationChunkSize, classLoader);

		CompletableFuture<List<AggregationChunk>> future = producer.streamTo(groupReducer)
				.getConsumerResult()
				.toCompletableFuture();

		eventloop.run();

		assertStatus(StreamStatus.END_OF_STREAM, producer);
		assertStatus(StreamStatus.END_OF_STREAM, groupReducer);
		assertEquals(future.get().size(), 5);

		for (StreamConsumer consumer : listConsumers) {
			assertStatus(StreamStatus.END_OF_STREAM, consumer);
		}
	}

}
