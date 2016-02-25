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

import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;
import org.junit.Test;

import java.nio.file.Paths;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
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
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ReportingDSLExpression d = divide(multiply(divide("a", "b"), 100), "c");
		TestQueryResultPlaceholder resultPlaceholder = new AsmBuilder<>(classLoader, TestQueryResultPlaceholder.class)
				.field("a", long.class)
				.field("b", long.class)
				.field("c", double.class)
				.field("d", double.class)
				.method("computeMeasures", set(getter(self(), "d"), d.getExpression()))
				.method("init", sequence(
						set(getter(self(), "a"), value(1)),
						set(getter(self(), "b"), value(100)),
						set(getter(self(), "c"), value(5))))
				.method("getResult", getter(self(), "d"))
				.newInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(newHashSet("a", "b", "c"), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ReportingDSLExpression d = divide(multiply(divide("a", "b"), 100), "c");
		TestQueryResultPlaceholder resultPlaceholder = new AsmBuilder<>(classLoader, TestQueryResultPlaceholder.class)
				.field("a", long.class)
				.field("b", long.class)
				.field("c", double.class)
				.field("d", double.class)
				.method("computeMeasures", set(getter(self(), "d"), d.getExpression()))
				.method("init", sequence(
						set(getter(self(), "a"), value(1)),
						set(getter(self(), "b"), value(0)),
						set(getter(self(), "c"), value(0))))
				.method("getResult", getter(self(), "d"))
				.newInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ReportingDSLExpression c = sqrt(add("a", "b"));
		TestQueryResultPlaceholder resultPlaceholder = new AsmBuilder<>(classLoader, TestQueryResultPlaceholder.class)
				.field("a", double.class)
				.field("b", double.class)
				.field("c", double.class)
				.method("computeMeasures", set(getter(self(), "c"), c.getExpression()))
				.method("init", sequence(
						set(getter(self(), "a"), value(2.0)),
						set(getter(self(), "b"), value(7.0))))
				.method("getResult", getter(self(), "c"))
				.newInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ReportingDSLExpression c = sqrt(subtract("a", "b"));
		TestQueryResultPlaceholder resultPlaceholder = new AsmBuilder<>(classLoader, TestQueryResultPlaceholder.class)
				.field("a", double.class)
				.field("b", double.class)
				.field("c", double.class)
				.method("computeMeasures", set(getter(self(), "c"), c.getExpression()))
				.method("init", sequence(
						set(getter(self(), "a"), value(0.0)),
						set(getter(self(), "b"), value(1E-10))))
				.method("getResult", getter(self(), "c"))
				.newInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
