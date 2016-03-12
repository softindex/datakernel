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

import java.net.HttpCookie;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StringUtilsTest {

	// test join
	@Test
	public void joinsStringsProperly() {
		String[] input = new String[]{"data1", "data2", "data3"};

		String resultOne = StringUtils.join(".", input);
		String resultTwo = StringUtils.join("!!!", input);

		assertEquals("data1.data2.data3", resultOne);
		assertEquals("data1!!!data2!!!data3", resultTwo);
	}

	@Test
	public void joinsObjectsProperly() {
		Object[] input = new Object[]{"data", Paths.get("file.txt"), 1, new HttpCookie("name", "value")};

		String result = StringUtils.join(", ", input);

		assertEquals("data, file.txt, 1, name=\"value\"", result);
	}

	@Test
	public void joinOperationThrowsNullPointerExceptionIfAtLeastOneInputStringsIsNull() {
		int exceptionCatchedTimes = 0;

		try {
			String[] inputOne = new String[]{null};
			StringUtils.join(";", inputOne);
		} catch (NullPointerException npe) {
			++exceptionCatchedTimes;
		}
		try {
			String[] inputTwo = new String[]{null, "data"};
			StringUtils.join(";", inputTwo);
		} catch (NullPointerException npe) {
			++exceptionCatchedTimes;
		}
		try {
			String[] inputThree = new String[]{"data", null, null, "data"};
			StringUtils.join(";", inputThree);
		} catch (NullPointerException npe) {
			++exceptionCatchedTimes;
		}

		assertEquals(3, exceptionCatchedTimes);
	}

	// test split
	@Test
	public void splitsStringCorrectly() {
		String input = "data1.data2.data3";

		String[] subStrings = StringUtils.split('.', input);

		String[] expectedSubStrings = new String[]{"data1", "data2", "data3"};
		assertArrayEquals(expectedSubStrings, subStrings);
	}

	@Test
	public void splitOperationReturnsInputStringWhenThereAreNoSeparators() {
		String input = "data";

		String[] subStrings = StringUtils.split('.', input);

		assertEquals(subStrings.length, 1);
		assertEquals(input, subStrings[0]);
	}

	@Test
	public void splitOperationOmitsEmptyStrings() {
		String input = "...data1.data2.....data3...";

		String[] expectedSubStrings = new String[]{"data1", "data2", "data3"};
		assertArrayEquals(expectedSubStrings, StringUtils.split('.', input));
	}

	@Test
	public void splitOperationConsidersAllSeparators() {
		String input = "data1.data2;data3;.data4";

		String[] subStrings = StringUtils.split(".;", input);

		String[] expectedSubStrings = new String[]{"data1", "data2", "data3", "data4"};
		assertArrayEquals(expectedSubStrings, subStrings);
	}

	@Test
	public void splitsToListProperly() {
		String input = "aaa,bbb;ccc.ddd";
		List<String> result = StringUtils.splitToList(";,.", input);

		assertEquals(4, result.size());
		assertEquals("aaa", result.get(0));
		assertEquals("bbb", result.get(1));
		assertEquals("ccc", result.get(2));
		assertEquals("ddd", result.get(3));
	}
}
