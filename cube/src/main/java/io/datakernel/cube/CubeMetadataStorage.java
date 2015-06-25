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

import com.google.common.base.Function;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.List;

/**
 * Manages persistence of cube metadata.
 */
public interface CubeMetadataStorage {
	/**
	 * Retrieves new unique id for chunk asynchronously.
	 *
	 * @param callback callback for consuming new chunk id
	 */
	void newChunkId(ResultCallback<Long> callback);

	/**
	 * Retrieves new unique id for chunk synchronously.
	 *
	 * @return new chunk id
	 */
	long newChunkId();

	/**
	 * Loads aggregations metadata from metadata storage to specified cube asynchronously.
	 *
	 * @param cube     cube where retrieved aggregations metadata is to be saved
	 * @param callback callback which is called once loading is complete
	 */
	void loadAggregations(Cube cube, CompletionCallback callback);

	/**
	 * Saves aggregations metadata from the specified cube to metadata storage asynchronously.
	 *
	 * @param cube     cube whose aggregations metadata is to be saved
	 * @param callback callback which is called once saving is complete
	 */
	void saveAggregations(Cube cube, CompletionCallback callback);

	/**
	 * Saves given chunks metadata to metadata storage asynchronously.
	 *
	 * @param aggregation aggregation which contains the specified chunks
	 * @param newChunks   list of chunks to save
	 * @param callback    callback which is called once saving is complete
	 */
	void saveChunks(Aggregation aggregation, List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback);

	/**
	 * Loads metadata of chunks, whose revision id is between {@code lastRevisionId} and {@code maxRevisionId}, to specified cube asynchronously.
	 *
	 * @param cube           cube where loaded chunks are to be saved
	 * @param lastRevisionId lower bound for revision id
	 * @param maxRevisionId  upper bound for revision id
	 * @param callback       callback which is called once loading is complete
	 */
	void loadChunks(Cube cube,
	                int lastRevisionId, int maxRevisionId,
	                ResultCallback<Integer> callback);

	/**
	 * Reloads all information about chunk consolidations to the given cube asynchronously.
	 *
	 * @param cube     cube where loaded information is to be saved
	 * @param callback callback which is called once loading is complete
	 */
	void reloadAllChunkConsolidations(Cube cube, CompletionCallback callback);

	/**
	 * Asynchronously saves the metadata of the given list of new chunks that are result of consolidation of the list of specified chunks.
	 *
	 * @param cube               cube which performed consolidation
	 * @param aggregation        aggregation that contains original and consolidated chunks
	 * @param originalChunks     list of original chunks
	 * @param consolidatedChunks list of chunks that appeared as a result of consolidation of original chunks
	 * @param callback           callback which is called once saving is complete
	 */
	void saveConsolidatedChunks(Cube cube, Aggregation aggregation,
	                            List<AggregationChunk> originalChunks,
	                            List<AggregationChunk.NewChunk> consolidatedChunks,
	                            CompletionCallback callback);

	/**
	 * Starts consolidation on the specified cube using the given consolidator function.
	 *
	 * @param cube              cube that contains the chunks that are to be consolidated
	 * @param chunkConsolidator function that starts consolidation
	 * @param callback          callback which is called once consolidation is started
	 */
	void performConsolidation(Cube cube, Function<List<Long>, List<AggregationChunk>> chunkConsolidator,
	                          CompletionCallback callback);

	/**
	 * Asynchronously refreshes consolidation information for chunks that are not currently known to be consolidated.
	 *
	 * @param cube     cube that contains the chunks
	 * @param callback callback which is called once consolidation information is refreshed
	 */
	void refreshNotConsolidatedChunks(Cube cube, CompletionCallback callback);

	/**
	 * Asynchronously removes metadata about the chunk with the given
	 *
	 * @param chunkId  id of chunk to be removed
	 * @param callback callback which is called once chunk is removed
	 */
	void removeChunk(long chunkId, CompletionCallback callback);
}
