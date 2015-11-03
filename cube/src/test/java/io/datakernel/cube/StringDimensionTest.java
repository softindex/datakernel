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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.aggregation_db.keytype.KeyTypeString;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.bean.DataItemResultString;
import io.datakernel.cube.bean.DataItemString1;
import io.datakernel.cube.bean.DataItemString2;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

public class StringDimensionTest {
	public static Cube newCube(Eventloop eventloop, DefiningClassLoader classLoader, AggregationChunkStorage storage,
	                           AggregationStructure structure) {
		AggregationMetadataStorageStub aggregationMetadataStorage = new AggregationMetadataStorageStub();
		Cube cube = new Cube(eventloop, classLoader, new LogToCubeMetadataStorageStub(aggregationMetadataStorage), aggregationMetadataStorage,
				storage, structure, 100_000, 1_000_000);
		cube.addAggregation(
				new AggregationMetadata("detailedAggregation", asList("key1", "key2"), asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static AggregationStructure cubeStructureWithStringDimension(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("key1", new KeyTypeString())
						.put("key2", new KeyTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("metric1", new FieldTypeLong())
						.put("metric2", new FieldTypeLong())
						.put("metric3", new FieldTypeLong())
						.build());
	}

	@Test
	public void testQuery() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure structure = cubeStructureWithStringDimension(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, structure);
		StreamProducers.ofIterable(eventloop, asList(new DataItemString1("str1", 2, 10, 20), new DataItemString1("str2", 3, 10, 20)))
				.streamTo(cube.consumer(DataItemString1.class, DataItemString1.DIMENSIONS, DataItemString1.METRICS, new CubeTest.MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItemString2("str2", 3, 10, 20), new DataItemString2("str1", 4, 10, 20)))
				.streamTo(cube.consumer(DataItemString2.class, DataItemString2.DIMENSIONS, DataItemString2.METRICS, new CubeTest.MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResultString> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResultString.class,
				new AggregationQuery()
						.keys("key1", "key2")
						.fields("metric1", "metric2", "metric3")
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
