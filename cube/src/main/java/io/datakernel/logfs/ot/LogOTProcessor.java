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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogPosition;
import io.datakernel.logfs.ot.LogDiff.LogPositionDiff;
import io.datakernel.ot.OTStateManager;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletionStage;

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
		callback.setComplete();
	}

	@Override
	public void stop(CompletionCallback callback) {
		callback.setComplete();
	}

	public CompletionStage<Void> rebase() {
		return stateManager.pull();
	}

	public CompletionStage<Void> processLog() {
		return stateManager.pull().thenCompose($ -> {
			logger.trace("processLog_gotPositions called. Positions: {}", state.positions);

			HashMap<String, LogPositionDiff> resultPositions = new LinkedHashMap<>();
			List<D> resultDataDiffs = new ArrayList<>();

			StreamProducer<T> producer = getProducer(resultPositions);

			return logStreamConsumer.consume(producer)
					.thenApply(diffs -> {
						resultDataDiffs.addAll(diffs);
						return LogDiff.of(resultPositions, resultDataDiffs);
					})
					.thenCompose(logDiff -> {
						logger.info("Log '{}' processing complete. Positions: {}", log, logDiff.positions);
						stateManager.add(logDiff);
						return stateManager.pull().thenCompose($2 -> stateManager.commitAndPush().thenApply(k -> null));
					});
		});
	}

	private StreamProducer<T> getProducer(HashMap<String, LogPositionDiff> resultPositions) {
		StreamUnion<T> streamUnion = StreamUnion.create(eventloop);
		for (String partition : this.partitions) {
			String logName = logName(partition);
			LogPosition logPosition = state.positions.get(logName);
			if (logPosition == null) {
				logPosition = LogPosition.create(new LogFile("", 0), 0L);
			}
			logger.info("Starting reading '{}' from position {}", logName, logPosition);

			LogPosition logPositionFrom = logPosition;
			logManager.producer(partition, logPosition.getLogFile(), logPosition.getPosition(),
					new ResultCallback<LogPosition>() {
						@Override
						protected void onResult(LogPosition logPositionTo) {
							if (!logPositionTo.equals(logPositionFrom)) {
								resultPositions.put(logName, new LogPositionDiff(logPositionFrom, logPositionTo));
							}
						}

						@Override
						protected void onException(Exception e) {
						}
					})
					.streamTo(streamUnion.newInput());
		}
		return streamUnion.getOutput();
	}

	private String logName(String partition) {
		return log != null && !log.isEmpty() ? log + "." + partition : partition;
	}

}
