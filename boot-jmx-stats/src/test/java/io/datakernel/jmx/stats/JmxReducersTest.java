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

package io.datakernel.jmx.stats;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.jmx.api.JmxReducers.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JmxReducersTest {

	@Test
	public void distinctReducerReturnsCommonValueIfAllValuesAreSame() {
		JmxReducerDistinct reducer = new JmxReducerDistinct();

		List<String> input = asList("data", "data", "data");

		assertEquals("data", reducer.reduce(input));
	}

	@Test
	public void distinctReducerReturnsNullIfThereAreAtLeastTwoDifferentValuesInInputList() {
		JmxReducerDistinct reducer = new JmxReducerDistinct();

		List<String> input = asList("data", "non-data", "data");

		assertNull(reducer.reduce(input));
	}

	@Test
	public void sumReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(10);
		numbers.add(15);

		int result = (int) sumReducer.reduce(numbers);
		assertEquals(25, result);
	}

	@Test
	public void sumReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);

		double result = (double) sumReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(7.5, result, acceptableError);
	}

	@Test
	public void sumReducerIgnoresNullValues() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(10);
		numbers.add(null);
		numbers.add(15);

		int result = (int) sumReducer.reduce(numbers);
		assertEquals(25, result);
	}

	@Test
	public void sumReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Number> numbers = new ArrayList<>();

		assertNull(sumReducer.reduce(numbers));
	}

	@Test
	public void minReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerMin minReducer = new JmxReducerMin();
		List<Number> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);
		numbers.add(10.0);

		double result = (double) minReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(2.5, result, acceptableError);
	}

	@Test
	public void minReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerMin minReducer = new JmxReducerMin();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(5);
		numbers.add(2);
		numbers.add(10);

		int result = (int) minReducer.reduce(numbers);
		assertEquals(2, result);
	}

	@Test
	public void minReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerMin minReducer = new JmxReducerMin();
		List<Number> numbers = new ArrayList<>();

		assertNull(minReducer.reduce(numbers));
	}

	@Test
	public void maxReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerMax maxReducer = new JmxReducerMax();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);
		numbers.add(10.0);

		double result = (double) maxReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(10.0, result, acceptableError);
	}

	@Test
	public void maxReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerMax maxReducer = new JmxReducerMax();
		List<Long> numbers = new ArrayList<>();
		numbers.add(5L);
		numbers.add(2L);
		numbers.add(10L);

		long result = (long) maxReducer.reduce(numbers);
		assertEquals(10L, result);
	}

	@Test
	public void maxReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerMin maxReducer = new JmxReducerMin();
		List<Number> numbers = new ArrayList<>();

		assertNull(maxReducer.reduce(numbers));
	}
}
