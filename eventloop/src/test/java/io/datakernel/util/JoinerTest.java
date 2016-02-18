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

import static org.junit.Assert.assertEquals;

public class JoinerTest {

	@Test
	public void itShouldJoinStrings() {
		String[] input = new String[]{"data1", "data2", "data3"};
		Joiner joinerOne = Joiner.on('.');
		Joiner joinerTwo = Joiner.on("!!!");

		String resultOne = joinerOne.join(input);
		String resultTwo = joinerTwo.join(input);

		assertEquals("data1.data2.data3", resultOne);
		assertEquals("data1!!!data2!!!data3", resultTwo);
	}

	@Test
	public void itShouldJoinObjects() {
		Object[] input = new Object[]{"data", Paths.get("file.txt"), 1, new HttpCookie("name", "value")};
		Joiner joinerOne = Joiner.on(", ");

		String resultOne = joinerOne.join(input);

		assertEquals("data, file.txt, 1, name=\"value\"", resultOne);
	}

	@Test
	public void itShouldThrowNullPointerExceptionIfAtLeastOneInputStringsIsNull() {
		String[] inputOne = new String[]{null};
		String[] inputTwo = new String[]{null, "data"};
		String[] inputThree = new String[]{"data", null, null, "data"};
		Joiner joiner = Joiner.on('.');
		int exceptionCatchedTimes = 0;

		try {
			joiner.join(inputOne);
		} catch (NullPointerException npe) {
			++exceptionCatchedTimes;
		}
		try {
			joiner.join(inputTwo);
		} catch (NullPointerException npe) {
			++exceptionCatchedTimes;
		}
		try {
			joiner.join(inputThree);
		} catch (NullPointerException npe) {
			++exceptionCatchedTimes;
		}

		assertEquals(3, exceptionCatchedTimes);
	}
}
