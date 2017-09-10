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

package io.datakernel.cube;

import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.DataItemResultString;
import io.datakernel.cube.bean.DataItemString1;
import io.datakernel.cube.bean.DataItemString2;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.and;
import static io.datakernel.aggregation.AggregationPredicates.eq;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StringDimensionTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testQuery() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		AggregationChunkStorage aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executor, new IdGeneratorStub(), aggregationsDir);
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("key1", FieldTypes.ofString())
				.withDimension("key2", FieldTypes.ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2").withMeasures("metric1", "metric2", "metric3"));

		StreamProducer<DataItemString1> producer1 = StreamProducers.ofIterable(eventloop, asList(
				new DataItemString1("str1", 2, 10, 20),
				new DataItemString1("str2", 3, 10, 20)));
		StreamConsumerWithResult<DataItemString1, CubeDiff> consumer1 = cube.consume(DataItemString1.class);
		producer1.streamTo(consumer1);
		CompletableFuture<CubeDiff> future1 = consumer1.getResult().toCompletableFuture();
		StreamProducer<DataItemString2> producer2 = StreamProducers.ofIterable(eventloop, asList(new DataItemString2("str2", 3, 10, 20), new DataItemString2("str1", 4, 10, 20)));
		StreamConsumerWithResult<DataItemString2, CubeDiff> consumer2 = cube.consume(DataItemString2.class);
		producer2.streamTo(consumer2);
		CompletableFuture<CubeDiff> future2 = consumer2.getResult().toCompletableFuture();
		eventloop.run();

		cube.apply(future1.get());
		cube.apply(future2.get());

		StreamConsumers.ToList<DataItemResultString> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", "str2"), eq("key2", 3)),
				DataItemResultString.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResultString> actual = consumerToList.getList();
		List<DataItemResultString> expected = asList(new DataItemResultString("str2", 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}
}
