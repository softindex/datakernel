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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.Aggregation;
import io.datakernel.aggregation_db.AggregationChunk;
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.cube.AggregatorSplitter;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.StreamUnion;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Processes logs. Creates new aggregation chunks and persists them using logic defined in supplied {@code AggregatorSplitter}.
 */
public final class LogToCubeRunner<T> {
	private final Eventloop eventloop;
	private final Cube cube;
	private final LogManager<T> logManager;
	private final LogToCubeMetadataStorage metadataStorage;
	private final AggregatorSplitter.Factory<T> aggregatorSplitterFactory;

	private final String log;
	private final List<String> partitions;

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Constructs a {@code LogToCubeRunner}, which runs in the given event loop, saves new chunks to specified cube,
	 * uses given log manager for reading logs, processes logs using an {@code AggregatorSplitter},
	 * that is instantiated through the specified factory, works with the given log and partitions,
	 * and uses the specified {@code LogToCubeMetadataStorage} for persistence of log and cube metadata.
	 *
	 * @param eventloop                 event loop to run in
	 * @param cube                      cube to save new chunks
	 * @param logManager                log manager for reading logs
	 * @param aggregatorSplitterFactory factory that instantiates an {@code AggregatorSplitter} subclass
	 * @param log                       log name
	 * @param partitions                list of partition names
	 * @param metadataStorage           metadata storage for persistence of log and cube metadata
	 */
	private LogToCubeRunner(Eventloop eventloop, Cube cube, LogManager<T> logManager,
	                        AggregatorSplitter.Factory<T> aggregatorSplitterFactory, String log, List<String> partitions,
	                        LogToCubeMetadataStorage metadataStorage) {
		this.eventloop = eventloop;
		this.metadataStorage = metadataStorage;
		this.cube = cube;
		this.logManager = logManager;
		this.aggregatorSplitterFactory = aggregatorSplitterFactory;
		this.log = log;
		this.partitions = ImmutableList.copyOf(partitions);
	}

	public static <T> LogToCubeRunner<T> create(Eventloop eventloop, Cube cube, LogManager<T> logManager,
	                                            AggregatorSplitter.Factory<T> aggregatorSplitterFactory,
	                                            String log, List<String> partitions,
	                                            LogToCubeMetadataStorage metadataStorage) {
		return new LogToCubeRunner<T>(eventloop, cube, logManager, aggregatorSplitterFactory,
				log, partitions, metadataStorage);
	}

	public void processLog(final CompletionCallback callback) {
		metadataStorage.loadLogPositions(log, partitions, new ForwardingResultCallback<Map<String, LogPosition>>(callback) {
			@Override
			public void onResult(Map<String, LogPosition> positions) {
				processLog_gotPositions(positions, callback);
			}
		});
	}

	private void processLog_gotPositions(Map<String, LogPosition> positions, final CompletionCallback callback) {
		logger.trace("processLog_gotPositions called. Positions: {}", positions);
		final Stopwatch sw = Stopwatch.createStarted();
		final AggregatorSplitter<T> aggregator = aggregatorSplitterFactory.create(eventloop);
		LogCommitTransaction<T> logCommitTransaction = LogCommitTransaction.create(eventloop, logManager, log, positions, new ForwardingLogCommitCallback(callback) {
			@Override
			public void onCommit(String log, Map<String, LogPosition> oldPositions,
			                     Map<String, LogPosition> newPositions,
			                     Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks) {
				processLog_doCommit(log, oldPositions, newPositions, newChunks, callback);

				sw.stop();
				logger.info("Aggregated {} objects from log '{}' into {} chunks in {} (~{} objects/second)",
						aggregator.getItems(), log, newChunks.size(), sw,
						(int) (aggregator.getItems() / ((double) sw.elapsed(TimeUnit.NANOSECONDS) / 1E9)));
			}
		});

		final StreamUnion<T> streamUnion = StreamUnion.create(eventloop);

		for (String logPartition : positions.keySet()) {
			LogPosition logPosition = positions.get(logPartition);
			if (logPosition == null) {
				logPosition = LogPosition.create();
			}
			logCommitTransaction.logProducer(logPartition, logPosition)
					.streamTo(streamUnion.newInput());
			logger.info("Started reading log '{}' for partition '{}' from position {}", log, logPartition, logPosition);
		}

		streamUnion.getOutput().streamTo(aggregator.getInput());

		aggregator.streamTo(cube, logCommitTransaction);
	}

	private void processLog_doCommit(final String log, Map<String, LogPosition> oldPositions,
	                                 Map<String, LogPosition> newPositions,
	                                 final Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks,
	                                 final CompletionCallback callback) {
		logger.trace("processLog_doCommit called. Log: {}. Old positions: {}. New positions: {}. New chunks: {}.", log, oldPositions, newPositions, newChunks);
		Map<AggregationMetadata, String> idMap = new HashMap<>();
		for (Map.Entry<String, Aggregation> entry : cube.getAggregations().entrySet()) {
			idMap.put(entry.getValue().getAggregationMetadata(), entry.getKey());
		}
		logger.info("Saving commit to metadata storage. Log: {}", log);
		metadataStorage.saveCommit(log, idMap, oldPositions, newPositions, newChunks,
				new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						logger.info("Completed saving commit to metadata storage. Log: {}", log);
						processLog_afterCommit(newChunks, callback);
					}
				});
	}

	private void processLog_afterCommit(Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		logger.trace("processLog_afterCommit called. New chunks: {}", newChunks);
		callback.onComplete();
	}
}
