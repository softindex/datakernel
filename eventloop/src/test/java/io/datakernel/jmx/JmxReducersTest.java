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

package io.datakernel.jmx;

import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxReducersTest {

	@Test
	public void distinctReducerReturnsCommonValueIfAllValuesAreSame() {
		JmxReducer<Object> reducer = JmxReducers.distinct();

		List<String> input = asList("data", "data", "data");

		assertEquals("data", reducer.reduce(input));
	}

	@Test
	public void distinctReducerReturnsNullIfThereAreAtLeastTwoDifferentValuesInInputList() {
		JmxReducer<Object> reducer = JmxReducers.distinct();

		List<String> input = asList("data", "non-data", "data");

		assertEquals(null, reducer.reduce(input));
	}
}
