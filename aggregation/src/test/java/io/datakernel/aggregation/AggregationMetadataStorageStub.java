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

import io.datakernel.async.AssertingResultCallback;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static java.util.Collections.EMPTY_LIST;

public class AggregationMetadataStorageStub implements AggregationMetadataStorage {
	private final Eventloop eventloop;
	private int revisionId;
	private long chunkId;
	private TreeMap<Integer, List<AggregationChunk>> revisionToChunks = new TreeMap<>();

	public AggregationMetadataStorageStub(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public AssertingResultCallback<List<AggregationChunk>> createSaveCallback() {
		return new AssertingResultCallback<List<AggregationChunk>>() {
			@Override
			protected void onResult(List<AggregationChunk> newChunks) {
				List<AggregationChunk> chunks = new ArrayList<>();
				chunks.addAll(newChunks);
				revisionToChunks.put(++revisionId, chunks);
			}
		};
	}

	@Override
	public void createChunkId(ResultCallback<Long> callback) {
		callback.postResult(eventloop, ++chunkId);
	}

	@Override
	public void startConsolidation(List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void loadChunks(Aggregation aggregation, int lastRevisionId, CompletionCallback callback) {
		List<AggregationChunk> allChunks = new ArrayList<>();
		for (List<AggregationChunk> chunks : revisionToChunks.tailMap(lastRevisionId, false).values()) {
			allChunks.addAll(chunks);
		}
		LoadedChunks loadedChunks = new LoadedChunks(revisionToChunks.lastKey(), EMPTY_LIST, allChunks);
		aggregation.loadChunks(loadedChunks, true);
		callback.postComplete(eventloop);
	}

	@Override
	public void saveConsolidatedChunks(List<AggregationChunk> originalChunks, List<AggregationChunk> consolidatedChunks, CompletionCallback callback) {
		throw new UnsupportedOperationException();
	}

}
