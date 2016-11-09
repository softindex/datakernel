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

import io.datakernel.aggregation_db.AggregationChunkStorageStub;
import io.datakernel.aggregation_db.CubeMetadataStorageStub;
import io.datakernel.aggregation_db.fieldtype.FieldTypes;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.DataItemResultString;
import io.datakernel.cube.bean.DataItemString1;
import io.datakernel.cube.bean.DataItemString2;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation_db.AggregationPredicates.and;
import static io.datakernel.aggregation_db.AggregationPredicates.eq;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation_db.processor.AggregateFunctions.sum;
import static io.datakernel.cube.Cube.AggregationScheme.id;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StringDimensionTest {

	@Test
	public void testQuery() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		Cube cube = Cube.create(eventloop, Executors.newCachedThreadPool(), classLoader, cubeMetadataStorage, storage)
				.withDimension("key1", FieldTypes.ofString())
				.withDimension("key2", FieldTypes.ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2").withMeasures("metric1", "metric2", "metric3"));
		StreamProducers.ofIterable(eventloop, asList(new DataItemString1("str1", 2, 10, 20), new DataItemString1("str2", 3, 10, 20)))
				.streamTo(cube.consumer(DataItemString1.class, DataItemString1.DIMENSIONS, DataItemString1.METRICS, new CubeTest.MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItemString2("str2", 3, 10, 20), new DataItemString2("str1", 4, 10, 20)))
				.streamTo(cube.consumer(DataItemString2.class, DataItemString2.DIMENSIONS, DataItemString2.METRICS, new CubeTest.MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResultString> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", "str2"), eq("key2", 3)),
				DataItemResultString.class, classLoader
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResultString> actual = consumerToList.getList();
		List<DataItemResultString> expected = asList(new DataItemResultString("str2", 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}
}
