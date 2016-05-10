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

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.bean.DataItemResultString;
import io.datakernel.cube.bean.DataItemString1;
import io.datakernel.cube.bean.DataItemString2;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation_db.fieldtype.FieldTypes.longSum;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static io.datakernel.aggregation_db.keytype.KeyTypes.stringKey;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StringDimensionTest {
	public static Cube newCube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                           AggregationChunkStorage storage, AggregationStructure structure) {
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, storage, structure,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE, Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY,
				Aggregation.DEFAULT_SORTER_BLOCK_SIZE, Cube.DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD,
				Aggregation.DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS);
		cube.addAggregation("detailedAggregation",
				new AggregationMetadata(asList("key1", "key2"), asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static AggregationStructure cubeStructureWithStringDimension() {
		return new AggregationStructure(
				ImmutableMap.<String, KeyType>builder()
						.put("key1", stringKey())
						.put("key2", intKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("metric1", longSum())
						.put("metric2", longSum())
						.put("metric3", longSum())
						.build());
	}

	@Test
	public void testQuery() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure structure = cubeStructureWithStringDimension();
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, structure);
		StreamProducers.ofIterable(eventloop, asList(new DataItemString1("str1", 2, 10, 20), new DataItemString1("str2", 3, 10, 20)))
				.streamTo(cube.consumer(DataItemString1.class, DataItemString1.DIMENSIONS, DataItemString1.METRICS, new CubeTest.MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItemString2("str2", 3, 10, 20), new DataItemString2("str1", 4, 10, 20)))
				.streamTo(cube.consumer(DataItemString2.class, DataItemString2.DIMENSIONS, DataItemString2.METRICS, new CubeTest.MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResultString> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResultString.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.eq("key1", "str2")
						.eq("key2", 3))
				.streamTo(consumerToList);
		eventloop.run();

		List<DataItemResultString> actual = consumerToList.getList();
		List<DataItemResultString> expected = asList(new DataItemResultString("str2", 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}
}
