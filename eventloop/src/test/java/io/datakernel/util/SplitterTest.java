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

package io.datakernel.util;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SplitterTest {

	@Test
	public void itShouldSplitStringCorrectly() {
		Splitter splitter = Splitter.on('.');
		String input = "data1.data2.data3";

		String[] subStrings = splitter.split(input);

		String[] expectedSubStrings = new String[]{"data1", "data2", "data3"};
		assertArrayEquals(expectedSubStrings, subStrings);
	}

	@Test
	public void itShouldReturnInputStringWhenThereAreNoSeparators() {
		Splitter splitter = Splitter.on('.');
		String input = "data";

		String[] subStrings = splitter.split(input);

		assertEquals(subStrings.length, 1);
		assertEquals(input, subStrings[0]);
	}

	@Test
	public void itShouldConsiderAllSeparators() {
		Splitter splitter = Splitter.onAnyOf(".;");
		String input = "data1.data2;data3;.data4";

		String[] subStrings = splitter.split(input);

		String[] expectedSubStrings = new String[]{"data1", "data2", "data3", "", "data4"};
		assertArrayEquals(expectedSubStrings, subStrings);
	}

	@Test
	public void subStringsAmountShouldBeOneMoreThanSeparatorsAmount() {
		char separator = '.';
		String word = "word";
		Splitter splitter = Splitter.on(separator);
		String input = separator + word + separator + word + separator + separator;

		String[] subStrings = splitter.split(input);

		int separatorsAmount = 4;
		assertEquals(separatorsAmount + 1, subStrings.length);
	}

	@Test
	public void testSplitToList() {
		String separators = ";,.";
		String input = "aaa,bbb;ccc.ddd";
		Splitter splitter = Splitter.onAnyOf(separators);

		List<String> result = splitter.splitToList(input);

		assertEquals(4, result.size());
		assertEquals("aaa", result.get(0));
		assertEquals("bbb", result.get(1));
		assertEquals("ccc", result.get(2));
		assertEquals("ddd", result.get(3));
	}

	@Test
	public void testTrim() {
		String separators = ";,.";
		String input = "aaa, bbb;    ccc    .      ddd   ";
		Splitter splitter = Splitter.onAnyOf(separators)
				.trimResults();

		List<String> list = splitter.splitToList(input);

		assertEquals(4, list.size());
		assertEquals("aaa", list.get(0));
		assertEquals("bbb", list.get(1));
		assertEquals("ccc", list.get(2));
		assertEquals("ddd", list.get(3));

		String[] array = splitter.split(input);

		assertEquals(4, array.length);
		assertEquals("aaa", array[0]);
		assertEquals("bbb", array[1]);
		assertEquals("ccc", array[2]);
		assertEquals("ddd", array[3]);

	}
}
