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
import com.google.common.collect.Collections2;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.aggregation_db.AggregationChunk.createChunk;

public class AggregationMetadataStorageStub implements AggregationMetadataStorage {
	private long chunkId;
	private Map<String, List<AggregationChunk.NewChunk>> tmpChunks = new HashMap<>();

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
		this.tmpChunks.put(aggregationMetadata.getId(), newChunks);
		callback.onComplete();
	}

	@Override
	public void startConsolidation(Aggregation aggregation, List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {

	}

	@Override
	public void loadChunks(Aggregation aggregation, final int lastRevisionId, ResultCallback<LoadedChunks> callback) {
		List<AggregationChunk.NewChunk> newChunks = tmpChunks.get(aggregation.getId());
		callback.onResult(new LoadedChunks(lastRevisionId + 1, Collections.<Long>emptyList(),
				Collections2.transform(newChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk input) {
						return createChunk(lastRevisionId, input);
					}
				})));
	}

	@Override
	public void saveConsolidatedChunks(Aggregation aggregation, AggregationMetadata aggregationMetadata, List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {

	}

}
