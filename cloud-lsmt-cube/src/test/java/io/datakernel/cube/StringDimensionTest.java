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
import io.datakernel.aggregation.ChunkIdCodec;
import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.DataItemResultString;
import io.datakernel.cube.bean.DataItemString1;
import io.datakernel.cube.bean.DataItemString2;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.and;
import static io.datakernel.aggregation.AggregationPredicates.eq;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class StringDimensionTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testQuery() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		AggregationChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, executor, aggregationsDir));
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("key1", ofString())
				.withDimension("key2", ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2").withMeasures("metric1", "metric2", "metric3"));

		CubeDiff consumer1Result = await(StreamSupplier.of(
				new DataItemString1("str1", 2, 10, 20),
				new DataItemString1("str2", 3, 10, 20))
				.streamTo(cube.consume(DataItemString1.class)));

		CubeDiff consumer2Result = await(StreamSupplier.of(
				new DataItemString2("str2", 3, 10, 20),
				new DataItemString2("str1", 4, 10, 20))
				.streamTo(cube.consume(DataItemString2.class)));

		await(aggregationChunkStorage.finish(consumer1Result.addedChunks().map(id -> (long) id).collect(toSet())));
		await(aggregationChunkStorage.finish(consumer2Result.addedChunks().map(id -> (long) id).collect(toSet())));

		cube.apply(consumer1Result);
		cube.apply(consumer2Result);

		StreamConsumerToList<DataItemResultString> consumerToList = StreamConsumerToList.create();
		await(cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", "str2"), eq("key2", 3)),
				DataItemResultString.class, DefiningClassLoader.create(classLoader))
				.streamTo(consumerToList));

		List<DataItemResultString> actual = consumerToList.getList();
		List<DataItemResultString> expected = singletonList(new DataItemResultString("str2", 3, 10, 30, 20));

		assertEquals(expected, actual);
	}
}
