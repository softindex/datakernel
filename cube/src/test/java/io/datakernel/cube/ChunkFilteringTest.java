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

import io.datakernel.aggregation.AggregationMetadata;
import io.datakernel.aggregation.PrimaryKey;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChunkFilteringTest {
	@Test
	public void testChunkMightContainQueryValues1() throws Exception {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(0, 3, 78, 10);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(10, 8, 79, 12);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 77, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 22);

		assertTrue(AggregationMetadata.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}

	@Test
	public void testChunkMightContainQueryValues2() throws Exception {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(3, 5, 81, 10);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(3, 5, 85, 12);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 77, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 22);

		assertFalse(AggregationMetadata.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}

	@Test
	public void testChunkMightContainQueryValues3() throws Exception {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(14, 5, 78, 10);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(20, 5, 79, 12);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 77, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 22);

		assertFalse(AggregationMetadata.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}

	@Test
	public void testChunkMightContainQueryValues4() throws Exception {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(3, 5, 80, 90);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(3, 5, 80, 90);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 80, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 90);

		assertTrue(AggregationMetadata.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}
}
