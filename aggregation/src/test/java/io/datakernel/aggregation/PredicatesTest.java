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

package io.datakernel.aggregation;

import org.junit.Test;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static org.junit.Assert.assertEquals;

public class PredicatesTest {
	@Test
	public void testSimplify() throws Exception {
		assertEquals(alwaysFalse(), and(eq("publisher", 10), eq("publisher", 20)).simplify());
		assertEquals(eq("publisher", 10), and(eq("publisher", 10), not(not(eq("publisher", 10)))).simplify());
		assertEquals(eq("publisher", 20), and(alwaysTrue(), eq("publisher", 20)).simplify());
		assertEquals(alwaysFalse(), and(alwaysFalse(), eq("publisher", 20)).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)), and(eq("date", 20160101), eq("publisher", 20)).simplify());

		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(not(not(and(not(not(eq("date", 20160101))), eq("publisher", 20)))), not(not(eq("publisher", 20)))).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(and(not(not(eq("publisher", 20))), not(not(eq("date", 20160101)))), and(eq("date", 20160101), eq("publisher", 20))).simplify());
	}

}