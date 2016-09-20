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
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConsolidationChunkSelectionTest {
	@Test
	public void testRangeExpansion() throws Exception {
		AggregationMetadata am = AggregationMetadata.create(singletonList(""), new ArrayList<String>());
		Set<AggregationChunk> chunks = newHashSet();
		chunks.add(createTestChunk(1, 1, 2));
		chunks.add(createTestChunk(2, 1, 2));
		chunks.add(createTestChunk(3, 1, 4));
		chunks.add(createTestChunk(4, 3, 4));
		chunks.add(createTestChunk(5, 3, 6));
		chunks.add(createTestChunk(6, 5, 6));
		chunks.add(createTestChunk(7, 5, 8));
		chunks.add(createTestChunk(8, 7, 8));

		addChunks(am, chunks);

		List<AggregationChunk> selectedChunks = am.findChunksForConsolidationHotSegment(100);
		assertEquals(chunks, newHashSet(selectedChunks));

		selectedChunks = am.findChunksForConsolidationHotSegment(5);
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
		AggregationMetadata am = AggregationMetadata.create(singletonList(""), new ArrayList<String>());

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

		addChunks(am, concat(chunks1, chunks2));

		List<AggregationChunk> selectedChunks = am.findChunksForConsolidationMinKey(100, 4000, 0);
		assertEquals(chunks1, newHashSet(selectedChunks));
	}

	@Test
	public void testSizeFixStrategy() throws Exception {
		AggregationMetadata am = AggregationMetadata.create(singletonList(""), new ArrayList<String>());
		int optimalChunkSize = 5;
		int maxChunks = 5;

		Set<AggregationChunk> chunks1 = newHashSet();
		chunks1.add(createTestChunk(1, 1, 2, optimalChunkSize));
		chunks1.add(createTestChunk(2, 3, 4, optimalChunkSize));

		Set<AggregationChunk> chunks2 = newHashSet();
		chunks2.add(createTestChunk(3, 5, 6, 4));
		chunks2.add(createTestChunk(4, 7, 8, 1));
		chunks2.add(createTestChunk(5, 9, 13, optimalChunkSize));
		chunks2.add(createTestChunk(6, 10, 11, optimalChunkSize));
		chunks2.add(createTestChunk(7, 10, 12, optimalChunkSize));

		Set<AggregationChunk> chunks3 = newHashSet();
		chunks3.add(createTestChunk(8, 14, 15, 3));
		chunks3.add(createTestChunk(9, 14, 15, 6));

		addChunks(am, concat(chunks1, chunks2, chunks3));

		List<AggregationChunk> selectedChunks = am.findChunksForConsolidationMinKey(maxChunks, optimalChunkSize, 0);
		assertEquals(chunks2, newHashSet(selectedChunks));
	}

	@Test
	public void testGroupingByPartition() throws Exception {
		AggregationMetadata am = AggregationMetadata.create(singletonList(""), new ArrayList<String>());

		Set<AggregationChunk> chunks1 = newHashSet();
		chunks1.add(createTestChunk(2, 1, 1, 1, 1, 1, 5));
		chunks1.add(createTestChunk(1, 1, 1, 1, 1, 1, 1));

		Set<AggregationChunk> chunks2 = newHashSet();
		chunks2.add(createTestChunk(3, 2, 2, 1, 1, 1, 1));
		chunks2.add(createTestChunk(4, 2, 2, 1, 1, 2, 2));

		Set<AggregationChunk> chunks3 = newHashSet();
		chunks3.add(createTestChunk(5, 2, 2, 2, 2, 3, 3));
		chunks3.add(createTestChunk(6, 2, 2, 2, 2, 1, 1));
		chunks3.add(createTestChunk(7, 2, 2, 2, 2, 1, 10));

		addChunks(am, concat(chunks1, chunks2, chunks3));

		Map<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> partitioningKeyToTree = am.groupByPartition(2);

		assertEquals(chunks1, partitioningKeyToTree.get(PrimaryKey.ofArray(1, 1)).getAll());
		assertEquals(chunks2, partitioningKeyToTree.get(PrimaryKey.ofArray(2, 1)).getAll());
		assertEquals(chunks3, partitioningKeyToTree.get(PrimaryKey.ofArray(2, 2)).getAll());

		am.addToIndex(createTestChunk(8, 1, 1, 2, 3, 5, 5));
		assertNull(am.groupByPartition(2));
	}

	private static void addChunks(AggregationMetadata am, Iterable<AggregationChunk> chunks) {
		for (AggregationChunk chunk : chunks) {
			am.addToIndex(chunk);
		}
	}

	private static AggregationChunk createTestChunk(int id, int min, int max) {
		return createTestChunk(id, min, max, id);
	}

	private static AggregationChunk createTestChunk(int id, int d1Min, int d1Max, int d2Min, int d2Max, int d3Min,
	                                                int d3Max) {
		return AggregationChunk.create(0, id, new ArrayList<String>(), PrimaryKey.ofArray(d1Min, d2Min, d3Min),
				PrimaryKey.ofArray(d1Max, d2Max, d3Max), 10);
	}

	private static AggregationChunk createTestChunk(int id, int min, int max, int count) {
		return AggregationChunk.create(0, id, new ArrayList<String>(), PrimaryKey.ofArray(min), PrimaryKey.ofArray(max), count);
	}
}
