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

package io.datakernel.cube.api;

import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.api.ReportingDSL.add;
import static io.datakernel.cube.api.ReportingDSL.*;
import static org.junit.Assert.assertEquals;

public class ComputedMeasuresTest {
	public interface TestQueryResultPlaceholder {
		void computeMeasures();

		void init();

		Object getResult();
	}

	@Test
	public void test() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ReportingDSLExpression d = divide(multiply(divide("a", "b"), 100), "c");
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(getter(self(), "d"), d.getExpression()))
				.withMethod("init", sequence(
						set(getter(self(), "a"), value(1)),
						set(getter(self(), "b"), value(100)),
						set(getter(self(), "c"), value(5))))
				.withMethod("getResult", getter(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(newHashSet("a", "b", "c"), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ReportingDSLExpression d = divide(multiply(divide("a", "b"), 100), "c");
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(getter(self(), "d"), d.getExpression()))
				.withMethod("init", sequence(
						set(getter(self(), "a"), value(1)),
						set(getter(self(), "b"), value(0)),
						set(getter(self(), "c"), value(0))))
				.withMethod("getResult", getter(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ReportingDSLExpression c = sqrt(add("a", "b"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(getter(self(), "c"), c.getExpression()))
				.withMethod("init", sequence(
						set(getter(self(), "a"), value(2.0)),
						set(getter(self(), "b"), value(7.0))))
				.withMethod("getResult", getter(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ReportingDSLExpression c = sqrt(subtract("a", "b"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(getter(self(), "c"), c.getExpression()))
				.withMethod("init", sequence(
						set(getter(self(), "a"), value(0.0)),
						set(getter(self(), "b"), value(1E-10))))
				.withMethod("getResult", getter(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
