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
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Expressions;
import io.datakernel.cube.ComputedMeasure;
import io.datakernel.cube.ComputedMeasures;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.ComputedMeasures.add;
import static io.datakernel.cube.ComputedMeasures.div;
import static io.datakernel.cube.ComputedMeasures.*;
import static io.datakernel.cube.ComputedMeasures.mul;
import static io.datakernel.cube.ComputedMeasures.sub;
import static org.junit.Assert.assertEquals;

public class ComputedMeasuresTest {
	private ComputedMeasure.StoredMeasures storedMeasures = new ComputedMeasure.StoredMeasures() {
		@Override
		public Class<?> getStoredMeasureType(String storedMeasureId) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Expression getStoredMeasureValue(Expression record, String storedMeasureId) {
			return field(record, storedMeasureId);
		}
	};

	public interface TestQueryResultPlaceholder {
		void computeMeasures();

		void init();

		Object getResult();
	}

	@Test
	public void test() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = div(mul(div(measure("a"), measure("b")), ComputedMeasures.value(100)), measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(field(self(), "d"), d.getExpression(self(), storedMeasures)))
				.withMethod("init", sequence(
						set(field(self(), "a"), Expressions.value(1)),
						set(field(self(), "b"), Expressions.value(100)),
						set(field(self(), "c"), Expressions.value(5))))
				.withMethod("getResult", field(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(newHashSet("a", "b", "c"), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = div(mul(div(measure("a"), measure("b")), ComputedMeasures.value(100)), measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", long.class)
				.withField("b", long.class)
				.withField("c", double.class)
				.withField("d", double.class)
				.withMethod("computeMeasures", set(field(self(), "d"), d.getExpression(self(), storedMeasures)))
				.withMethod("init", sequence(
						set(field(self(), "a"), Expressions.value(1)),
						set(field(self(), "b"), Expressions.value(0)),
						set(field(self(), "c"), Expressions.value(0))))
				.withMethod("getResult", field(self(), "d"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = sqrt(add(measure("a"), measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(field(self(), "c"), c.getExpression(self(), storedMeasures)))
				.withMethod("init", sequence(
						set(field(self(), "a"), Expressions.value(2.0)),
						set(field(self(), "b"), Expressions.value(7.0))))
				.withMethod("getResult", field(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = sqrt(sub(measure("a"), measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(field(self(), "c"), c.getExpression(self(), storedMeasures)))
				.withMethod("init", sequence(
						set(field(self(), "a"), Expressions.value(0.0)),
						set(field(self(), "b"), Expressions.value(1E-10))))
				.withMethod("getResult", field(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
