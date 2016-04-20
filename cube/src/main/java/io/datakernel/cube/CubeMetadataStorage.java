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
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.aggregation_db.AggregationMetadataStorage;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.async.ResultCallback;

import java.util.List;
import java.util.Map;

public interface CubeMetadataStorage {
	AggregationMetadataStorage aggregationMetadataStorage(String aggregationId, AggregationMetadata aggregationMetadata,
	                                                      AggregationStructure aggregationStructure);

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

	void loadChunks(int lastRevisionId, Map<String, AggregationMetadata> aggregations,
	                AggregationStructure aggregationStructure, ResultCallback<CubeLoadedChunks> callback);
}
