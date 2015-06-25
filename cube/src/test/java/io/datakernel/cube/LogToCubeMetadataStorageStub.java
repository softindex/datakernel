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
import com.google.common.collect.Multimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.logfs.LogPosition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Maps.filterKeys;

public final class LogToCubeMetadataStorageStub implements LogToCubeMetadataStorage {
	private int revisionId;
	private static long chunkId;
	private Map<String, Map<String, LogPosition>> positions = new LinkedHashMap<>();

	private Map<String, LogPosition> ensureLogPositions(String log) {
		Map<String, LogPosition> logPositionMap = positions.get(log);
		if (logPositionMap == null) {
			logPositionMap = new LinkedHashMap<>();
			positions.put(log, logPositionMap);
		}
		return logPositionMap;
	}

	@Override
	public void loadLogPositions(String log, List<String> partitions, ResultCallback<Map<String, LogPosition>> callback) {
		Map<String, LogPosition> logPositionMap = new LinkedHashMap<>();
		for (String partition : partitions) {
			logPositionMap.put(partition, new LogPosition());
		}
		logPositionMap.putAll(ensureLogPositions(log));
		callback.onResult(filterKeys(logPositionMap, in(partitions)));
	}

	@Override
	public void commit(Cube cube, String log, Map<String, LogPosition> oldPositions, Map<String, LogPosition> newPositions, Multimap<Aggregation, AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		Map<String, LogPosition> logPositionMap = ensureLogPositions(log);
		logPositionMap.putAll(newPositions);
		callback.onComplete();
		cube.incrementLastRevisionId();
		for (Map.Entry<Aggregation, AggregationChunk.NewChunk> entry : newChunks.entries()) {
			Aggregation aggregation = entry.getKey();
			AggregationChunk.NewChunk newChunk = entry.getValue();
			aggregation.addToIndex(AggregationChunk.createCommitChunk(cube.getLastRevisionId(), newChunk));
		}
	}

	@Override
	public void saveAggregations(Cube cube, CompletionCallback callback) {
	}

	@Override
	public void saveChunks(Aggregation aggregation, List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {

	}

	@Override
	public void loadChunks(Cube cube, int lastRevisionId, int maxRevisionId, ResultCallback<Integer> callback) {
	}

	@Override
	public void reloadAllChunkConsolidations(Cube cube, CompletionCallback callback) {

	}

	@Override
	public void saveConsolidatedChunks(Cube cube, Aggregation aggregation, List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
	}

	@Override
	public void performConsolidation(Cube cube, Function<List<Long>, List<AggregationChunk>> chunkConsolidator, CompletionCallback callback) {

	}

	@Override
	public void refreshNotConsolidatedChunks(Cube cube, CompletionCallback callback) {

	}

	@Override
	public void removeChunk(long chunkId, CompletionCallback callback) {

	}

	@Override
	public void newChunkId(ResultCallback<Long> callback) {
		callback.onResult(++chunkId);
	}

	@Override
	public long newChunkId() {
		return ++chunkId;
	}

	@Override
	public void loadAggregations(Cube cube, CompletionCallback callback) {
	}
}
