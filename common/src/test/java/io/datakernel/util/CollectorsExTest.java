/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import java.util.stream.Collector;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class CollectorsExTest {
	@Test
	public void testToVoid() {
		Void collect = Stream.of(1, 2, 3).collect(CollectorsEx.toVoid());
		assertNull(collect);
	}

	@Test
	public void testToFirst() {
		Integer collect = Stream.of(1, 2, 3).collect(CollectorsEx.toFirst());
		assertEquals(1, collect.intValue());
	}

	@Test
	public void testTolast() {
		Integer collect = Stream.of(1, 2, 3).collect(CollectorsEx.toLast());
		assertEquals(3, collect.intValue());
	}

	@Test
	public void testToArray() {
		Integer[] collect = Stream.of(1, 2, 3).collect(CollectorsEx.toArray(Integer.class));
		assertArrayEquals(new Integer[]{1, 2, 3}, collect);
	}

	@Test
	public void testToAll() {
		Collector<Boolean, boolean[], Boolean> collector = CollectorsEx.toAll();
		Boolean collect = Stream.of(true, true).collect(collector);
		assertTrue(collect);

		collect = Stream.of(true, false).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, true).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, false).collect(collector);
		assertFalse(collect);
	}

	@Test
	public void testToAllWithPredicate() {
		Boolean collect = Stream.of(2, 4).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(2, 5).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 4).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 5).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertFalse(collect);
	}

	@Test
	public void testToAny() {
		Collector<Boolean, boolean[], Boolean> collector = CollectorsEx.toAny();

		Boolean collect = Stream.of(true, true).collect(collector);
		assertTrue(collect);

		collect = Stream.of(true, false).collect(collector);
		assertTrue(collect);

		collect = Stream.of(false, true).collect(collector);
		assertTrue(collect);

		collect = Stream.of(false, false).collect(collector);
		assertFalse(collect);
	}

	@Test
	public void testToAnyWithPredicate() {
		Boolean collect = Stream.of(2, 4).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(2, 5).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(3, 4).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(3, 5).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertFalse(collect);
	}

	@Test
	public void testToNone() {
		Collector<Boolean, boolean[], Boolean> collector = CollectorsEx.toNone();

		Boolean collect = Stream.of(true, true).collect(collector);
		assertFalse(collect);

		collect = Stream.of(true, false).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, true).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, false).collect(collector);
		assertTrue(collect);
	}

	@Test
	public void testToNoneWithPredicate() {
		Boolean collect = Stream.of(2, 4).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(2, 5).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 4).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 5).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertTrue(collect);
	}

}
