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

import com.google.common.base.Function;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.List;

public class AggregationMetadataStorageStub implements AggregationMetadataStorage {
	private static long chunkId;

	@Override
	public void newChunkId(ResultCallback<Long> callback) {
		callback.onResult(++chunkId);
	}

	@Override
	public long newChunkId() {
		return ++chunkId;
	}

	@Override
	public void saveAggregationMetadata(Aggregation aggregation, AggregationStructure structure, CompletionCallback callback) {

	}

	@Override
	public void saveChunks(AggregationMetadata aggregationMetadata, List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {

	}

	@Override
	public int loadChunks(Aggregation aggregation, int lastRevisionId, int maxRevisionId) {
		return 0;
	}

	@Override
	public void loadChunks(Aggregation aggregation, int lastRevisionId, int maxRevisionId, ResultCallback<Integer> callback) {

	}

	@Override
	public void reloadAllChunkConsolidations(Aggregation aggregation, CompletionCallback callback) {

	}

	@Override
	public void saveConsolidatedChunks(Aggregation aggregation, AggregationMetadata aggregationMetadata, List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {

	}

	@Override
	public void performConsolidation(Aggregation aggregation, Function<List<Long>, List<AggregationChunk>> chunkConsolidator, CompletionCallback callback) {

	}

	@Override
	public void refreshNotConsolidatedChunks(Aggregation aggregation, CompletionCallback callback) {

	}

	@Override
	public void removeChunk(long chunkId, CompletionCallback callback) {

	}
}
