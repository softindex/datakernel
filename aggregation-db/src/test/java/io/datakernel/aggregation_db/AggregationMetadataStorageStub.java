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
import java.util.List;

import static io.datakernel.aggregation_db.AggregationChunk.createChunk;

public class AggregationMetadataStorageStub implements AggregationMetadataStorage {
	private long chunkId;
	private List<AggregationChunk.NewChunk> tmpChunks;

	@Override
	public void createChunkId(ResultCallback<Long> callback) {
		callback.sendResult(++chunkId);
	}

	public long newChunkId() {
		return ++chunkId;
	}

	@Override
	public void saveChunks(List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		this.tmpChunks = newChunks;
		callback.complete();
	}

	@Override
	public void startConsolidation(List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
		callback.complete();
	}

	@Override
	public void loadChunks(final int lastRevisionId, ResultCallback<LoadedChunks> callback) {
		callback.sendResult(new LoadedChunks(lastRevisionId + 1, Collections.<Long>emptyList(),
				Collections2.transform(tmpChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk input) {
						return createChunk(lastRevisionId, input);
					}
				})));
	}

	@Override
	public void saveConsolidatedChunks(List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
		callback.complete();
	}

}
