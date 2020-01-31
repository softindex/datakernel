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

package io.datakernel.cube.http;

import io.datakernel.aggregation.measure.Measure;
import io.datakernel.aggregation.measure.Measures;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.ComputedMeasure;
import io.datakernel.cube.ComputedMeasures;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.collection.CollectionUtils.keysToMap;
import static org.junit.Assert.assertEquals;

public class ComputedMeasuresTest {
	private static final class M extends Measures {}

	private static final class CM extends ComputedMeasures {}

	public interface TestQueryResultPlaceholder {
		void computeMeasures();

		void init();

		Object getResult();
	}

	private static final Map<String, Measure> MEASURES =
			keysToMap(Stream.of("a", "b", "c", "d"), k -> M.sum(ofDouble()));

	@Test
	public void test() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = CM.div(CM.mul(CM.div(CM.measure("a"), CM.measure("b")), CM.value(100)), CM.measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(property(self(), "d"), d.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(1)),
						set(property(self(), "b"), value(100)),
						set(property(self(), "c"), value(5))))
				.withMethod("getResult", property(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(Stream.of("a", "b", "c").collect(Collectors.toSet()), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = CM.div(CM.mul(CM.div(CM.measure("a"), CM.measure("b")), CM.value(100)), CM.measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(property(self(), "d"), d.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(1)),
						set(property(self(), "b"), value(0)),
						set(property(self(), "c"), value(0))))
				.withMethod("getResult", property(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = CM.sqrt(CM.add(CM.measure("a"), CM.measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(2.0)),
						set(property(self(), "b"), value(7.0))))
				.withMethod("getResult", property(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = CM.sqrt(CM.sub(CM.measure("a"), CM.measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(0.0)),
						set(property(self(), "b"), value(1E-10))))
				.withMethod("getResult", property(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
