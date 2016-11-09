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

package io.datakernel.logfs;

import com.google.common.collect.Multimap;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;

import java.util.List;
import java.util.Map;

/**
 * Manages persistence of cube and log metadata.
 */
public interface LogToCubeMetadataStorage {
	/**
	 * Asynchronously loads log positions for the given log name and list of names of partitions.
	 *
	 * @param log        name of log file
	 * @param partitions list of names of partitions
	 * @param callback   callback for consuming result in the form of 'log name'-'{@code LogPosition}' pairs
	 */
	void loadLogPositions(Cube cube, String log, List<String> partitions,
	                      ResultCallback<Map<String, LogPosition>> callback);

	/**
	 * Commits information about processing log to metadata storage.
	 * Updates (in storage) old log positions with the new values, contained in the list of new log positions.
	 * Saves metadata on new chunks, that appeared as a result of processing log, to the given cube.
	 * @param log          name of log file
	 * @param oldPositions old log positions
	 * @param newPositions new log positions
	 * @param newChunksByAggregation    metadata on new chunks
	 * @param callback     callback which is called once committing is complete
	 */
	void saveCommit(Cube cube, String log,
	                Map<String, LogPosition> oldPositions,
	                Map<String, LogPosition> newPositions,
	                Multimap<String, AggregationChunk.NewChunk> newChunksByAggregation,
	                CompletionCallback callback);
}
