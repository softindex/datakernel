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

package io.datakernel.aggregation_db;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class ConsolidationChunkSelectionTest {
	@Test
	public void testRangeExpansion() throws Exception {
		AggregationMetadata am = new AggregationMetadata(singletonList(""), new ArrayList<String>());
		Set<AggregationChunk> chunks = newHashSet();
		chunks.add(createTestChunk(1, 1, 2));
		chunks.add(createTestChunk(2, 1, 2));
		chunks.add(createTestChunk(3, 1, 4));
		chunks.add(createTestChunk(4, 3, 4));
		chunks.add(createTestChunk(5, 3, 6));
		chunks.add(createTestChunk(6, 5, 6));
		chunks.add(createTestChunk(7, 5, 8));
		chunks.add(createTestChunk(8, 7, 8));

		for (AggregationChunk chunk : chunks) {
			am.addToIndex(chunk);
		}

		List<AggregationChunk> selectedChunks = am.findChunksForConsolidation(100, 1.0);
		assertEquals(chunks, newHashSet(selectedChunks));

		selectedChunks = am.findChunksForConsolidation(5, 1.0);
		assertEquals(5, selectedChunks.size());

		chunks.clear();
		chunks.add(createTestChunk(3, 1, 4));
		chunks.add(createTestChunk(4, 3, 4));
		chunks.add(createTestChunk(5, 3, 6));
		chunks.add(createTestChunk(6, 5, 6));
		chunks.add(createTestChunk(7, 5, 8));

		assertEquals(chunks, newHashSet(selectedChunks));
	}

	@Test
	public void testMinKeyStrategy() throws Exception {
		AggregationMetadata am = new AggregationMetadata(singletonList(""), new ArrayList<String>());

		Set<AggregationChunk> chunks1 = newHashSet();
		chunks1.add(createTestChunk(1, 1, 2));
		chunks1.add(createTestChunk(2, 1, 2));
		chunks1.add(createTestChunk(3, 1, 4));
		chunks1.add(createTestChunk(4, 3, 4));

		Set<AggregationChunk> chunks2 = newHashSet();
		chunks2.add(createTestChunk(9, 9, 10));
		chunks2.add(createTestChunk(10, 9, 10));
		chunks2.add(createTestChunk(11, 10, 11));
		chunks2.add(createTestChunk(12, 10, 13));
		chunks2.add(createTestChunk(13, 12, 13));

		Set<AggregationChunk> allChunks = newHashSet(concat(chunks1, chunks2));
		for (AggregationChunk chunk : allChunks) {
			am.addToIndex(chunk);
		}

		List<AggregationChunk> selectedChunks = am.findChunksForConsolidation(100, 0.0);
		assertEquals(chunks1, newHashSet(selectedChunks));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testChunkGroups() throws Exception {
		AggregationMetadata am = new AggregationMetadata(singletonList(""), new ArrayList<String>());

		Set<AggregationChunk> chunks1 = newHashSet();
		chunks1.add(createTestChunk(1, 1, 2));
		chunks1.add(createTestChunk(2, 1, 2));
		chunks1.add(createTestChunk(3, 1, 4));
		chunks1.add(createTestChunk(4, 3, 4));
		chunks1.add(createTestChunk(5, 3, 6));
		chunks1.add(createTestChunk(6, 5, 6));
		chunks1.add(createTestChunk(7, 5, 8));
		chunks1.add(createTestChunk(8, 7, 8));

		Set<AggregationChunk> chunks2 = newHashSet();
		chunks2.add(createTestChunk(9, 9, 10));
		chunks2.add(createTestChunk(10, 9, 10));
		chunks2.add(createTestChunk(11, 10, 11));
		chunks2.add(createTestChunk(12, 10, 13));
		chunks2.add(createTestChunk(13, 12, 13));

		Set<AggregationChunk> chunks3 = newHashSet();
		chunks3.add(createTestChunk(14, 14, 15));
		chunks3.add(createTestChunk(15, 14, 16));
		chunks3.add(createTestChunk(16, 15, 16));

		Set<AggregationChunk> allChunks = newHashSet(concat(chunks1, chunks2, chunks3));
		for (AggregationChunk chunk : allChunks) {
			am.addToIndex(chunk);
		}

		List<List<AggregationChunk>> chunksForConsolidation = am.findChunkGroupsForConsolidation(100, 10);
		assertEquals(newHashSet(chunks1, chunks2, chunks3), toSet(chunksForConsolidation));

		chunksForConsolidation = am.findChunkGroupsForConsolidation(100, 2);
		assertEquals(newHashSet(chunks1, chunks2), toSet(chunksForConsolidation));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testTrimmedChunkGroups() throws Exception {
		AggregationMetadata am = new AggregationMetadata(singletonList(""), new ArrayList<String>());

		Set<AggregationChunk> chunks1 = newHashSet();
		chunks1.add(createTestChunk(1, 1, 2));
		chunks1.add(createTestChunk(2, 1, 2));
		chunks1.add(createTestChunk(3, 1, 4));
		chunks1.add(createTestChunk(4, 3, 4));
		chunks1.add(createTestChunk(5, 3, 6));
		chunks1.add(createTestChunk(6, 5, 6));
		chunks1.add(createTestChunk(7, 5, 8));
		chunks1.add(createTestChunk(8, 7, 8));

		Set<AggregationChunk> chunks2 = newHashSet();
		chunks2.add(createTestChunk(9, 9, 10));
		chunks2.add(createTestChunk(10, 9, 10));
		chunks2.add(createTestChunk(11, 10, 11));
		chunks2.add(createTestChunk(12, 10, 13));
		chunks2.add(createTestChunk(13, 12, 13));

		Set<AggregationChunk> chunks3 = newHashSet();
		chunks3.add(createTestChunk(14, 14, 15));
		chunks3.add(createTestChunk(15, 14, 16));
		chunks3.add(createTestChunk(16, 15, 16));

		Set<AggregationChunk> allChunks = newHashSet(concat(chunks1, chunks2, chunks3));
		for (AggregationChunk chunk : allChunks) {
			am.addToIndex(chunk);
		}

		List<List<AggregationChunk>> chunksForConsolidation = am.findChunkGroupsForConsolidation(5, 3);
		chunks1.clear();
		chunks1.add(createTestChunk(1, 1, 2));
		chunks1.add(createTestChunk(2, 1, 2));
		chunks1.add(createTestChunk(3, 1, 4));
		chunks1.add(createTestChunk(4, 3, 4));
		chunks1.add(createTestChunk(5, 3, 6));

		chunks2.clear();
		chunks2.add(createTestChunk(9, 9, 10));
		chunks2.add(createTestChunk(10, 9, 10));
		chunks2.add(createTestChunk(11, 10, 11));
		chunks2.add(createTestChunk(12, 10, 13));
		chunks2.add(createTestChunk(13, 12, 13));
		assertEquals(newHashSet(chunks1, chunks2, chunks3), toSet(chunksForConsolidation));
	}

	private static AggregationChunk createTestChunk(int id, int min, int max) {
		return new AggregationChunk(0, id, new ArrayList<String>(), PrimaryKey.ofArray(min), PrimaryKey.ofArray(max), id);
	}

	private static Set<Set<AggregationChunk>> toSet(List<List<AggregationChunk>> chunkGroups) {
		Set<Set<AggregationChunk>> set = newHashSet();
		for (List<AggregationChunk> chunkGroup : chunkGroups) {
			Set<AggregationChunk> chunkSet = newHashSet();
			for (AggregationChunk chunk : chunkGroup) {
				chunkSet.add(chunk);
			}
			set.add(chunkSet);
		}
		return set;
	}
}
