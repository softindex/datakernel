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

package io.datakernel.aggregation;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.Collection;
import java.util.List;

/**
 * Manages persistence of aggregation metadata.
 */
public interface AggregationMetadataStorage {
	/**
	 * Retrieves new unique id for chunk asynchronously.
	 *
	 * @param callback callback for consuming new chunk id
	 */
	void createChunkId(ResultCallback<Long> callback);

	final class LoadedChunks {
		public final int lastRevisionId;
		public final Collection<Long> consolidatedChunkIds;
		public final Collection<AggregationChunk> newChunks;

		public LoadedChunks(int lastRevisionId, Collection<Long> consolidatedChunkIds, Collection<AggregationChunk> newChunks) {
			this.lastRevisionId = lastRevisionId;
			this.consolidatedChunkIds = consolidatedChunkIds;
			this.newChunks = newChunks;
		}
	}

	/**
	 * Loads metadata of chunks, whose revision id is after {@code lastRevisionId}, to specified {@link Aggregation} asynchronously.
	 *  @param lastRevisionId lower bound for revision id
	 * @param callback       callback which is called once loading is complete
	 */
	void loadChunks(int lastRevisionId, ResultCallback<LoadedChunks> callback);

	void startConsolidation(List<AggregationChunk> chunksToConsolidate, CompletionCallback callback);

	/**
	 * Asynchronously saves the metadata of the given list of new chunks that are result of consolidation of the list of specified chunks.
	 * @param originalChunks      list of original chunks
	 * @param consolidatedChunks  list of chunks that appeared as a result of consolidation of original chunks
	 * @param callback            callback which is called once saving is complete
	 */
	void saveConsolidatedChunks(List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks,
	                            CompletionCallback callback);
}
