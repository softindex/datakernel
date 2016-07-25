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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.datakernel.jmx.Utils.concat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class UtilsTest {

	@Test
	public void concatMethodWorksProperlyWithNoIterables() {
		List<Iterable<String>> listWithNoIterables = new ArrayList<>();

		Iterable<String> commonIterable = concat(listWithNoIterables);
		Iterator<String> commonIterator = commonIterable.iterator();

		assertFalse(commonIterator.hasNext());
	}

	@Test
	public void concatMethodWorksProperlyWithSingleEmptyIterable() {
		List<String> emptyStringList = new ArrayList<>();
		List<Iterable<String>> listOfIterables = Arrays.<Iterable<String>>asList(emptyStringList);

		Iterable<String> commonIterable = concat(listOfIterables);
		Iterator<String> commonIterator = commonIterable.iterator();

		assertFalse(commonIterator.hasNext());
	}

	@Test
	public void concatMethodWorksProperlyWithSingleNonEmptyIterable() {
		List<String> stringList = asList("a", "b");
		List<Iterable<String>> listOfIterables = Arrays.<Iterable<String>>asList(stringList);

		Iterable<String> commonIterable = concat(listOfIterables);
		Iterator<String> commonIterator = commonIterable.iterator();

		assertTrue(commonIterator.hasNext());
		assertEquals("a", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("b", commonIterator.next());

		assertFalse(commonIterator.hasNext());
	}

	@Test
	public void concatMethodWorksProperlyWithSeveralEmptyIterables() {
		List<String> emptyStringList_1 = new ArrayList<>();
		List<String> emptyStringList_2 = new ArrayList<>();
		List<String> emptyStringList_3 = new ArrayList<>();
		List<Iterable<String>> listOfIterables =
				Arrays.<Iterable<String>>asList(emptyStringList_1, emptyStringList_2, emptyStringList_3);

		Iterable<String> commonIterable = concat(listOfIterables);
		Iterator<String> commonIterator = commonIterable.iterator();

		assertFalse(commonIterator.hasNext());
	}

	@Test
	public void concatMethodWorksProperlyWithNonEmptyIterables() {
		List<String> stringList_1 = asList("a", "b");
		List<String> stringList_2 = singletonList("c");
		List<String> stringList_3 = asList("d", "e");
		List<Iterable<String>> listOfIterables =
				Arrays.<Iterable<String>>asList(stringList_1, stringList_2, stringList_3);

		Iterable<String> commonIterable = concat(listOfIterables);
		Iterator<String> commonIterator = commonIterable.iterator();

		assertTrue(commonIterator.hasNext());
		assertEquals("a", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("b", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("c", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("d", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("e", commonIterator.next());

		assertFalse(commonIterator.hasNext());
	}

	@Test
	public void concatMethodWorksProperlyWithBothEmptyAndNonEmptyIterables() {
		List<String> stringList_1 = asList("a", "b");
		List<String> emptyStringList_2 = new ArrayList<>();
		List<String> stringList_3 = singletonList("s");
		List<Iterable<String>> listOfIterables =
				Arrays.<Iterable<String>>asList(stringList_1, emptyStringList_2, stringList_3);

		Iterable<String> commonIterable = concat(listOfIterables);
		Iterator<String> commonIterator = commonIterable.iterator();

		assertTrue(commonIterator.hasNext());
		assertEquals("a", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("b", commonIterator.next());

		assertTrue(commonIterator.hasNext());
		assertEquals("s", commonIterator.next());

		assertFalse(commonIterator.hasNext());
	}
}
