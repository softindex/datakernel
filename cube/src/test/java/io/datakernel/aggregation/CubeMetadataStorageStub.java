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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeMetadataStorage;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

public class CubeMetadataStorageStub implements CubeMetadataStorage {
	private long chunkId;
	private Map<String, List<AggregationChunk>> tmpChunks = new HashMap<>();

	public long newChunkId() {
		return ++chunkId;
	}

	@Override
	public void createChunkId(Cube cube, String aggregationId, ResultCallback<Long> callback) {
		callback.setResult(++chunkId);
	}

	@Override
	public void startConsolidation(Cube cube, String aggregationId, List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
		callback.setComplete();
	}

	@Override
	public void saveConsolidatedChunks(Cube cube, String aggregationId, List<AggregationChunk> originalChunks, List<AggregationChunk> consolidatedChunks, CompletionCallback callback) {
		callback.setComplete();
	}

	@Override
	public void loadChunks(Cube cube, final int lastRevisionId, CompletionCallback callback) {
		Map<String, List<AggregationChunk>> newChunks = new HashMap<>();

		for (String aggregationId : cube.getAggregationIds()) {
			List<AggregationChunk> chunks = tmpChunks.get(aggregationId);

			if (chunks == null)
				chunks = new ArrayList<>();

			newChunks.put(aggregationId, chunks);
		}

		CubeLoadedChunks cubeLoadedChunks = new CubeLoadedChunks(lastRevisionId + 1, Collections.<String, List<Long>>emptyMap(), newChunks);
		cube.loadChunksIntoAggregations(cubeLoadedChunks, true);

		callback.postComplete();
	}

	public void doSaveChunk(String aggregationId, List<AggregationChunk> newChunks, CompletionCallback callback) {
		tmpChunks.put(aggregationId, newChunks);
		callback.setComplete();
	}
}
