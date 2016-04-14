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

import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.AggregationChunk;
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.aggregation_db.CubeMetadataStorageStub;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.logfs.LogPosition;
import io.datakernel.logfs.LogToCubeMetadataStorage;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Maps.filterKeys;
import static java.util.Collections.singletonList;

public final class LogToCubeMetadataStorageStub implements LogToCubeMetadataStorage {
	private Map<String, Map<String, LogPosition>> positions = new LinkedHashMap<>();
	private final CubeMetadataStorageStub cubeMetadataStorage;

	public LogToCubeMetadataStorageStub(CubeMetadataStorageStub cubeMetadataStorage) {
		this.cubeMetadataStorage = cubeMetadataStorage;
	}

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
		callback.sendResult(filterKeys(logPositionMap, in(partitions)));
	}

	@Override
	public void saveCommit(String log, Map<AggregationMetadata, String> idMap,
	                       Map<String, LogPosition> oldPositions,
	                       Map<String, LogPosition> newPositions,
	                       Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks,
	                       CompletionCallback callback) {
		Map<String, LogPosition> logPositionMap = ensureLogPositions(log);
		logPositionMap.putAll(newPositions);
		callback.complete();
		for (Map.Entry<AggregationMetadata, AggregationChunk.NewChunk> entry : newChunks.entries()) {
			AggregationChunk.NewChunk newChunk = entry.getValue();
			String aggregationId = idMap.get(entry.getKey());
			cubeMetadataStorage.doSaveChunk(aggregationId, singletonList(newChunk), callback);
		}
	}

}
