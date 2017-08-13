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

package io.datakernel.logfs.ot;

import io.datakernel.aggregation.util.AsyncResultsReducer;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogPosition;
import io.datakernel.logfs.ot.LogDiff.LogPositionDiff;
import io.datakernel.ot.OTStateManager;
import io.datakernel.stream.processor.StreamUnion;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Processes logs. Creates new aggregation chunks and persists them using logic defined in supplied {@code AggregatorSplitter}.
 */
public final class LogOTProcessor<K, T, D> implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final LogManager<T> logManager;
	private final LogDataConsumer<T, D> logStreamConsumer;

	private final String log;
	private final List<String> partitions;

	private final OTStateManager<K, LogDiff<D>> stateManager;
	private final LogOTState<D> state;

	private LogOTProcessor(Eventloop eventloop, LogManager<T> logManager, LogDataConsumer<T, D> logStreamConsumer, String log, List<String> partitions,
	                       OTStateManager<K, LogDiff<D>> stateManager, LogOTState<D> state) {
		this.eventloop = eventloop;
		this.logManager = logManager;
		this.logStreamConsumer = logStreamConsumer;
		this.log = log;
		this.partitions = partitions;
		this.stateManager = stateManager;
		this.state = state;
	}

	public static <K, T, D> LogOTProcessor<K, T, D> create(Eventloop eventloop, LogManager<T> logManager, LogDataConsumer<T, D> logStreamConsumer, String log, List<String> partitions,
	                                                       OTStateManager<K, LogDiff<D>> stateManager) {
		return new LogOTProcessor<>(eventloop, logManager, logStreamConsumer, log, partitions, stateManager,
				(LogOTState<D>) stateManager.getState());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		stateManager.start(callback);
	}

	@Override
	public void stop(CompletionCallback callback) {
		stateManager.stop(callback);
	}

	public void rebase(CompletionCallback callback) {
		stateManager.rebase(callback);
	}

	public void processLog(final CompletionCallback callback) {
		stateManager.rebase(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				processLog_gotPositions(callback);
			}
		});
	}

	private String logName(String partition) {
		return log != null && !log.isEmpty() ? log + "." + partition : partition;
	}

	private void processLog_gotPositions(final CompletionCallback callback) {
		logger.trace("processLog_gotPositions called. Positions: {}", state.positions);
		final Stopwatch sw = Stopwatch.createStarted();

		final HashMap<String, LogPositionDiff> resultPositions = new LinkedHashMap<>();
		final List<D> resultDataDiffs = new ArrayList<>();
		AsyncResultsReducer<LogDiff<D>> resultsReducer = AsyncResultsReducer.create(LogDiff.of(resultPositions, resultDataDiffs));

		final StreamUnion<T> streamUnion = StreamUnion.create(eventloop);
		for (final String partition : this.partitions) {
			final String logName = logName(partition);
			LogPosition logPosition = state.positions.get(logName);
			if (logPosition == null) {
				logPosition = LogPosition.create(new LogFile("", 0), 0L);
			}
			logger.info("Starting reading '{}' from position {}", logName, logPosition);
			final LogPosition logPositionFrom = logPosition;
			logManager.producer(partition, logPosition.getLogFile(), logPosition.getPosition(),
					resultsReducer.newResultCallback(new AsyncResultsReducer.ResultReducer<LogDiff<D>, LogPosition>() {
						@Override
						public LogDiff<D> applyResult(LogDiff<D> accumulator, LogPosition logPositionTo) {
							if (!logPositionTo.equals(logPositionFrom)) {
								resultPositions.put(logName, new LogPositionDiff(logPositionFrom, logPositionTo));
							}
							return accumulator;
						}
					}))
					.streamTo(streamUnion.newInput());
		}

		logStreamConsumer.consume(streamUnion.getOutput(),
				resultsReducer.newResultCallback(new AsyncResultsReducer.ResultReducer<LogDiff<D>, List<D>>() {
					@Override
					public LogDiff<D> applyResult(LogDiff<D> accumulator, List<D> diffs) {
						resultDataDiffs.addAll(diffs);
						return accumulator;
					}
				}));

		resultsReducer.setResultTo(new ForwardingResultCallback<LogDiff<D>>(callback) {
			@Override
			public void onResult(final LogDiff<D> result1) {
				sw.stop();
				logger.info("Log '{}' processing complete in {}. Positions: {}", log, sw, result1.positions);
				stateManager.apply(result1);
				stateManager.rebase(new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						stateManager.commitAndPush(callback);
					}
				});
			}
		});
	}

}
