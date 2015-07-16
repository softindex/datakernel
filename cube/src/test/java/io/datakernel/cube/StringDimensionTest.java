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
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.CubeTest.MyCommitCallback;
import io.datakernel.cube.dimensiontype.DimensionType;
import io.datakernel.cube.dimensiontype.DimensionTypeInt;
import io.datakernel.cube.dimensiontype.DimensionTypeString;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StringDimensionTest {
	public static Cube newCube(Eventloop eventloop, DefiningClassLoader classLoader, AggregationStorage storage,
	                           CubeStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, new LogToCubeMetadataStorageStub(), storage, cubeStructure);
		cube.addAggregation(
				new Aggregation("detailedAggregation", asList("key1", "key2"), asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static CubeStructure cubeStructureWithStringDimension(DefiningClassLoader classLoader) {
		return new CubeStructure(classLoader,
				ImmutableMap.<String, DimensionType>builder()
						.put("key1", new DimensionTypeString())
						.put("key2", new DimensionTypeInt())
						.build(),
				ImmutableMap.<String, MeasureType>builder()
						.put("metric1", MeasureType.SUM_LONG)
						.put("metric2", MeasureType.SUM_LONG)
						.put("metric3", MeasureType.SUM_LONG)
						.build());
	}

	@Test
	public void testQuery() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructureWithStringDimension(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItemString1("str1", 2, 10, 20), new DataItemString1("str2", 3, 10, 20)))
				.streamTo(cube.consumer(DataItemString1.class, DataItemString1.DIMENSIONS, DataItemString1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItemString2("str2", 3, 10, 20), new DataItemString2("str1", 4, 10, 20)))
				.streamTo(cube.consumer(DataItemString2.class, DataItemString2.DIMENSIONS, DataItemString2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResultString> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResultString.class,
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
