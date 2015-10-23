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

import io.datakernel.aggregation_db.api.ReportingDSLExpression;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.aggregation_db.api.ReportingDSL.divide;
import static io.datakernel.aggregation_db.api.ReportingDSL.multiply;
import static io.datakernel.codegen.Expressions.*;
import static org.junit.Assert.assertEquals;

public class ReportingTest {
	public interface TestQueryResultPlaceholder {
		void compute();

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
				.method("compute", set(field(self(), "d"), d.getExpression()))
				.method("init", sequence(
						set(field(self(), "a"), value(1)),
						set(field(self(), "b"), value(100)),
						set(field(self(), "c"), value(5))))
				.method("getResult", field(self(), "d"))
				.newInstance();
		resultPlaceholder.init();
		resultPlaceholder.compute();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(newHashSet("a", "b", "c"), d.getMeasureDependencies());
	}
}
