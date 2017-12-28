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
import io.datakernel.async.Stages;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
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
import java.util.function.Function;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.measure.Measures.union;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"Duplicates", "unchecked", "ArraysAsListWithZeroOrOneArgument"})
public class AggregationGroupReducerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException, ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = new AggregationStructure()
				.withKey("word", FieldTypes.ofString())
				.withMeasure("documents", union(ofInt()));

		final List<StreamConsumer> listConsumers = new ArrayList<>();
		final List items = new ArrayList();
		AggregationChunkStorage aggregationChunkStorage = new AggregationChunkStorage() {
			long chunkId;

			@Override
			public <T> CompletionStage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				return Stages.of(StreamProducers.withEndOfStream(StreamProducers.ofIterator(eventloop, items.iterator())));
			}

			@Override
			public <T> CompletionStage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList consumer = new StreamConsumerToList<>(eventloop, items);
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

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<?> keyClass = AggregationUtils.createKeyClass(structure, asList("word"), classLoader);
		Class<?> aggregationClass = AggregationUtils.createRecordClass(structure, asList("word"), asList("documents"), classLoader);

		Function<InvertedIndexRecord, Comparable<?>> keyFunction = AggregationUtils.createKeyFunction(inputClass, keyClass,
				asList("word"), classLoader);

		Aggregate aggregate = AggregationUtils.createPreaggregator(structure, inputClass, aggregationClass,
				singletonMap("word", "word"), singletonMap("documents", "documentId"), classLoader);

		int aggregationChunkSize = 2;

		StreamProducer<InvertedIndexRecord> producer = StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3),
				new InvertedIndexRecord("brown", 3), new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1), new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10)));

		AggregationGroupReducer<InvertedIndexRecord> groupReducer = new AggregationGroupReducer<>(eventloop, aggregationChunkStorage,
				structure, asList("documents"),
				aggregationClass, AggregationUtils.singlePartition(), keyFunction, aggregate, aggregationChunkSize, classLoader);

		producer.streamTo(groupReducer);
		CompletableFuture<List<AggregationChunk>> future = groupReducer.getResult().toCompletableFuture();

		eventloop.run();

		assertStatus(StreamStatus.END_OF_STREAM, producer);
		assertStatus(StreamStatus.END_OF_STREAM, groupReducer);
		assertEquals(future.get().size(), 5);

		for (StreamConsumer consumer : listConsumers) {
			assertStatus(StreamStatus.END_OF_STREAM, consumer);
		}
	}

}
