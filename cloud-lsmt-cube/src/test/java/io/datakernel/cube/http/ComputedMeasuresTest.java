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
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expressions;
import io.datakernel.cube.ComputedMeasure;
import io.datakernel.cube.ComputedMeasures;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.ComputedMeasures.add;
import static io.datakernel.cube.ComputedMeasures.div;
import static io.datakernel.cube.ComputedMeasures.*;
import static io.datakernel.cube.ComputedMeasures.mul;
import static io.datakernel.cube.ComputedMeasures.sub;
import static io.datakernel.util.CollectionUtils.keysToMap;
import static org.junit.Assert.assertEquals;

public class ComputedMeasuresTest {

	public interface TestQueryResultPlaceholder {
		void computeMeasures();

		void init();

		Object getResult();
	}

	private static final Map<String, Measure> MEASURES =
			keysToMap(Stream.of("a", "b", "c", "d"), k -> sum(ofDouble()));

	@Test
	public void test() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = div(mul(div(measure("a"), measure("b")), ComputedMeasures.value(100)), measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(property(self(), "d"), d.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), Expressions.value(1)),
						set(property(self(), "b"), Expressions.value(100)),
						set(property(self(), "c"), Expressions.value(5))))
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
		ComputedMeasure d = div(mul(div(measure("a"), measure("b")), ComputedMeasures.value(100)), measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(property(self(), "d"), d.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), Expressions.value(1)),
						set(property(self(), "b"), Expressions.value(0)),
						set(property(self(), "c"), Expressions.value(0))))
				.withMethod("getResult", property(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = sqrt(add(measure("a"), measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), Expressions.value(2.0)),
						set(property(self(), "b"), Expressions.value(7.0))))
				.withMethod("getResult", property(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = sqrt(sub(measure("a"), measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), Expressions.value(0.0)),
						set(property(self(), "b"), Expressions.value(1E-10))))
				.withMethod("getResult", property(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
