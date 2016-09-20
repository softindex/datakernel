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

package io.datakernel.cube;

import com.google.common.collect.ImmutableSet;
import io.datakernel.aggregation_db.RangeTree;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeTreeTest {

	@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
	@Test
	public void testPut() throws Exception {
		RangeTree<Integer, String> rangeTree = RangeTree.create();
		rangeTree.put(1, 10, "a");
		rangeTree.put(3, 8, "b");
		rangeTree.put(5, 5, "c");
		rangeTree = RangeTree.cloneOf(rangeTree);
		assertEquals(ImmutableSet.of(), rangeTree.get(0));
		assertEquals(ImmutableSet.of("a"), rangeTree.get(1));
		assertEquals(ImmutableSet.of("a"), rangeTree.get(2));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.get(3));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.get(4));
		assertEquals(ImmutableSet.of("a", "b", "c"), rangeTree.get(5));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.get(6));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.get(8));
		assertEquals(ImmutableSet.of("a"), rangeTree.get(9));
		assertEquals(ImmutableSet.of("a"), rangeTree.get(10));
		assertEquals(ImmutableSet.of(), rangeTree.get(11));

		assertEquals(ImmutableSet.of(), rangeTree.getRange(0, 0));
		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(1, 1));
		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(2, 2));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(3, 3));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(4, 4));
		assertEquals(ImmutableSet.of("a", "b", "c"), rangeTree.getRange(5, 5));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(6, 6));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(8, 8));
		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(9, 9));
		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(10, 10));
		assertEquals(ImmutableSet.of(), rangeTree.getRange(11, 11));

		assertEquals(ImmutableSet.of(), rangeTree.getRange(0, 0));
		assertEquals(ImmutableSet.of("a", "b", "c"), rangeTree.getRange(0, 11));

		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(1, 1));
		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(1, 2));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(1, 3));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(1, 4));
		assertEquals(ImmutableSet.of("a", "b", "c"), rangeTree.getRange(1, 5));

		assertEquals(ImmutableSet.of("a", "b", "c"), rangeTree.getRange(5, 11));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(6, 11));
		assertEquals(ImmutableSet.of("a", "b"), rangeTree.getRange(8, 11));
		assertEquals(ImmutableSet.of("a"), rangeTree.getRange(9, 11));
		assertEquals(ImmutableSet.of(), rangeTree.getRange(11, 11));
	}

	@Test
	public void testRemove() throws Exception {
		RangeTree<Integer, String> rangeTree = RangeTree.create();

		rangeTree.put(1, 20, "a");
		rangeTree.put(5, 15, "b");
		rangeTree.put(5, 10, "c");
		rangeTree.put(10, 15, "d");
		assertEquals(ImmutableSet.of("a", "b", "c", "d"), rangeTree.getRange(0, 20));

		rangeTree.remove(5, 10, "c");
		rangeTree.remove(10, 15, "d");
		rangeTree.remove(1, 20, "a");
		rangeTree.remove(5, 15, "b");
		assertEquals(ImmutableSet.of(), rangeTree.getRange(0, 20));
		assertTrue(rangeTree.getSegments().isEmpty());
	}
}