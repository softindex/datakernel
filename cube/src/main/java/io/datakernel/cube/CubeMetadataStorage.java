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

import io.datakernel.aggregation_db.AggregationChunk;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CubeMetadataStorage {
	void createChunkId(Cube cube, String aggregationId, ResultCallback<Long> callback);

	void startConsolidation(Cube cube, String aggregationId, List<AggregationChunk> chunksToConsolidate, CompletionCallback callback);

	void saveConsolidatedChunks(Cube cube, String aggregationId,
	                            List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks,
	                            CompletionCallback callback);


	final class CubeLoadedChunks {
		public final int lastRevisionId;
		public final Map<String, List<Long>> consolidatedChunkIds;
		public final Map<String, List<AggregationChunk>> newChunks;

		public CubeLoadedChunks(int lastRevisionId, Map<String, List<Long>> consolidatedChunkIds,
		                        Map<String, List<AggregationChunk>> newChunks) {
			this.lastRevisionId = lastRevisionId;
			this.consolidatedChunkIds = consolidatedChunkIds;
			this.newChunks = newChunks;
		}
	}

	void loadChunks(Cube cube, int lastRevisionId, Set<String> aggregations,
	                ResultCallback<CubeLoadedChunks> callback);
}
