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

import io.datakernel.async.Stages;
import io.datakernel.async.StagesAccumulator;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogPosition;
import io.datakernel.logfs.ot.LogDiff.LogPositionDiff;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.Stages.onResult;

/**
 * Processes logs. Creates new aggregation chunks and persists them using logic defined in supplied {@code AggregatorSplitter}.
 */
public final class LogOTProcessor<T, D> implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final LogManager<T> logManager;
	private final LogDataConsumer<T, D> logStreamConsumer;

	private final String log;
	private final List<String> partitions;

	private final LogOTState<D> state;

	private LogOTProcessor(Eventloop eventloop, LogManager<T> logManager, LogDataConsumer<T, D> logStreamConsumer,
	                       String log, List<String> partitions, LogOTState<D> state) {
		this.eventloop = eventloop;
		this.logManager = logManager;
		this.logStreamConsumer = logStreamConsumer;
		this.log = log;
		this.partitions = partitions;
		this.state = state;
	}

	public static <T, D> LogOTProcessor<T, D> create(Eventloop eventloop, LogManager<T> logManager,
	                                                 LogDataConsumer<T, D> logStreamConsumer,
	                                                 String log, List<String> partitions, LogOTState<D> state) {
		return new LogOTProcessor<>(eventloop, logManager, logStreamConsumer, log, partitions, state);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public CompletionStage<Void> start() {
		return Stages.of(null);
	}

	@Override
	public CompletionStage<Void> stop() {
		return Stages.of(null);
	}

	@SuppressWarnings("unchecked")
	public CompletionStage<LogDiff<D>> processLog() {
		logger.trace("processLog_gotPositions called. Positions: {}", state.positions);

		StreamProducerWithResult<T, Map<String, LogPositionDiff>> producer = getProducer();
		StreamConsumerWithResult<T, List<D>> consumer = logStreamConsumer.consume();
		producer.streamTo(consumer);

		return Stages.pair(producer.getResult(), consumer.getResult())
				.thenApply(pair -> LogDiff.of(pair.getLeft(), pair.getRight()))
				.whenComplete(onResult(logDiff ->
						logger.info("Log '{}' processing complete. Positions: {}", log, logDiff.positions)));
	}

	private StreamProducerWithResult<T, Map<String, LogPositionDiff>> getProducer() {
		StagesAccumulator<Map<String, LogPositionDiff>> result = StagesAccumulator.create(new HashMap<>());
		StreamUnion<T> streamUnion = StreamUnion.create(eventloop);
		for (String partition : this.partitions) {
			String logName = logName(partition);
			LogPosition logPosition = state.positions.get(logName);
			if (logPosition == null) {
				logPosition = LogPosition.create(new LogFile("", 0), 0L);
			}
			logger.info("Starting reading '{}' from position {}", logName, logPosition);

			LogPosition logPositionFrom = logPosition;
			StreamProducerWithResult<T, LogPosition> producer = logManager.producerStream(partition, logPosition.getLogFile(), logPosition.getPosition(), null);
			producer.streamTo(streamUnion.newInput());
			result.addStage(producer.getResult(), (accumulator, logPositionTo) -> {
				if (!logPositionTo.equals(logPositionFrom)) {
					accumulator.put(logName, new LogPositionDiff(logPositionFrom, logPositionTo));
				}
			});
		}
		return StreamProducers.withResult(streamUnion.getOutput(), result.get());
	}

	private String logName(String partition) {
		return log != null && !log.isEmpty() ? log + "." + partition : partition;
	}

}
