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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.AggregationChunk;
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class LogCommitTransaction<T> {

	private final Eventloop eventloop;
	private final LogManager<T> logManager;
	private final String log;
	private final Map<String, LogPosition> oldPositions;

	private ListMultimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks = LinkedListMultimap.create();
	private Map<String, LogPosition> newPositions = new LinkedHashMap<>();

	private int commitCallbacks;
	private int logCallbacks;

	private final LogCommitCallback callback;

	private static final Logger logger = LoggerFactory.getLogger(LogCommitTransaction.class);

	public LogCommitTransaction(Eventloop eventloop, LogManager<T> logManager, String log, Map<String, LogPosition> oldPositions,
	                            LogCommitCallback callback) {
		this.eventloop = eventloop;
		this.logManager = logManager;
		this.log = log;
		this.oldPositions = new LinkedHashMap<>(oldPositions);
		this.callback = callback;
	}

	public ResultCallback<Multimap<AggregationMetadata, AggregationChunk.NewChunk>> addCommitCallback() {
		commitCallbacks++;
		logger.trace("Added commit callback. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks);
		return new ResultCallback<Multimap<AggregationMetadata, AggregationChunk.NewChunk>>() {
			@Override
			public void onResult(Multimap<AggregationMetadata, AggregationChunk.NewChunk> resultChunks) {
				newChunks.putAll(resultChunks);
				commitCallbacks--;
				logger.trace("Commit callback onCommit called. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks);
				tryCommit();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Commit callback onException called. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks, exception);
			}
		};
	}

	public ResultCallback<Map<String, LogPosition>> addLogCallback() {
		logCallbacks++;
		logger.trace("Added log callback. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks);
		return new ResultCallback<Map<String, LogPosition>>() {
			@Override
			public void onResult(Map<String, LogPosition> resultPositions) {
				logCallbacks--;
				logger.trace("Log callback onResult called. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks);
				newPositions.putAll(resultPositions);
				tryCommit();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Log callback onException called. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks, exception);
				callback.onException(exception);
			}
		};
	}

	private void tryCommit() {
		logger.trace("tryCommit called.");
		if (commitCallbacks != 0 || logCallbacks != 0) {
			logger.trace("Exiting tryCommit. Commit callbacks: {}. Log callbacks: {}", commitCallbacks, logCallbacks);
			return;
		}
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.onCommit(log, oldPositions, newPositions, newChunks);
			}
		});
	}

	@SuppressWarnings("unchecked")
	public StreamProducer<T> logProducer(final String logPartition, final LogPosition logPosition) {
		final ResultCallback<Map<String, LogPosition>> logCallback = addLogCallback();
		return logManager.producer(logPartition, logPosition.getLogFile(), logPosition.getPosition(),
				new ResultCallback<LogPosition>() {
					@Override
					public void onResult(LogPosition result) {
						Map<String, LogPosition> map = new HashMap<>();
						map.put(logPartition, result);
						logCallback.onResult(map);
					}

					@Override
					public void onException(Exception exception) {
						logCallback.onException(exception);
						logger.error("Log producer exception. Log partition: {}. Log position: {}", logPartition, logPosition);
					}
				});
	}

}
