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
import io.datakernel.cube.CubeMetadataStorage;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.aggregation_db.AggregationChunk.createChunk;

public class CubeMetadataStorageStub implements CubeMetadataStorage {
	private long chunkId;
	private Map<String, List<AggregationChunk.NewChunk>> tmpChunks = new HashMap<>();

	public long newChunkId() {
		return ++chunkId;
	}

	@Override
	public AggregationMetadataStorage aggregationMetadataStorage(final String aggregationId, final AggregationMetadata aggregationMetadata, AggregationStructure aggregationStructure) {
		return new AggregationMetadataStorage() {
			@Override
			public void createChunkId(ResultCallback<Long> callback) {
				callback.sendResult(++chunkId);
			}

			@Override
			public void saveChunks(List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
				doSaveChunk(aggregationId, newChunks, callback);
			}

			@Override
			public void startConsolidation(List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
				callback.complete();
			}

			@Override
			public void loadChunks(final int lastRevisionId, ResultCallback<AggregationMetadataStorage.LoadedChunks> callback) {
				List<AggregationChunk.NewChunk> newChunks = tmpChunks.get(aggregationId);

				if (newChunks == null)
					newChunks = new ArrayList<>();

				callback.sendResult(new LoadedChunks(lastRevisionId + 1, Collections.<Long>emptyList(),
						Collections2.transform(newChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
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
		};
	}

	@Override
	public void loadChunks(final int lastRevisionId, Map<String, AggregationMetadata> aggregations, AggregationStructure aggregationStructure, ResultCallback<CubeLoadedChunks> callback) {
		Map<String, List<AggregationChunk>> newChunks = new HashMap<>();

		for (String aggregationId : aggregations.keySet()) {
			List<AggregationChunk.NewChunk> chunks = tmpChunks.get(aggregationId);

			if (chunks == null)
				chunks = new ArrayList<>();

			newChunks.put(aggregationId, newArrayList(Collections2.transform(chunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
				@Override
				public AggregationChunk apply(AggregationChunk.NewChunk input) {
					return createChunk(lastRevisionId, input);
				}
			})));
		}

		callback.sendResult(new CubeLoadedChunks(lastRevisionId + 1, Collections.<String, List<Long>>emptyMap(), newChunks));
	}

	public void doSaveChunk(String aggregationId, List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		tmpChunks.put(aggregationId, newChunks);
		callback.complete();
	}
}
