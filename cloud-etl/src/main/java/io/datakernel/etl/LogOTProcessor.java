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

package io.datakernel.etl;

import io.datakernel.async.AsyncCollector;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.multilog.LogFile;
import io.datakernel.multilog.LogPosition;
import io.datakernel.multilog.Multilog;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamSupplierWithResult;
import io.datakernel.stream.processor.StreamUnion;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Processes logs. Creates new aggregation chunks and persists them using logic defined in supplied {@code AggregatorSplitter}.
 */
@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public final class LogOTProcessor<T, D> implements EventloopService, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(LogOTProcessor.class);

	private final Eventloop eventloop;
	private final Multilog<T> multilog;
	private final LogDataConsumer<T, D> logStreamConsumer;

	private final String log;
	private final List<String> partitions;

	private final LogOTState<D> state;

	// JMX
	private boolean enabled = true;
	private boolean detailed;
	private final StreamStatsBasic<T> streamStatsBasic = StreamStats.basic();
	private final StreamStatsDetailed<T> streamStatsDetailed = StreamStats.detailed();
	private final PromiseStats promiseProcessLog = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats promiseSupplier = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats promiseConsumer = PromiseStats.create(Duration.ofMinutes(5));

	private LogOTProcessor(Eventloop eventloop, Multilog<T> multilog, LogDataConsumer<T, D> logStreamConsumer,
			String log, List<String> partitions, LogOTState<D> state) {
		this.eventloop = eventloop;
		this.multilog = multilog;
		this.logStreamConsumer = logStreamConsumer;
		this.log = log;
		this.partitions = partitions;
		this.state = state;
	}

	public static <T, D> LogOTProcessor<T, D> create(Eventloop eventloop, Multilog<T> multilog,
			LogDataConsumer<T, D> logStreamConsumer,
			String log, List<String> partitions, LogOTState<D> state) {
		return new LogOTProcessor<>(eventloop, multilog, logStreamConsumer, log, partitions, state);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private final AsyncSupplier<LogDiff<D>> processLog = reuse(this::doProcessLog);

	public Promise<LogDiff<D>> processLog() {
		return processLog.get();
	}

	@NotNull
	private Promise<LogDiff<D>> doProcessLog() {
		if (!enabled) return Promise.of(LogDiff.of(emptyMap(), emptyList()));
		logger.trace("processLog_gotPositions called. Positions: {}", state.getPositions());

		StreamSupplierWithResult<T, Map<String, LogPositionDiff>> supplier = getSupplier();
		StreamConsumerWithResult<T, List<D>> consumer = logStreamConsumer.consume();
		return supplier.getSupplier().streamTo(consumer.getConsumer())
				.thenCompose($ -> Promises.toTuple(
						supplier.getResult().whenComplete(promiseSupplier.recordStats()),
						consumer.getResult().whenComplete(promiseConsumer.recordStats())))
				.whenComplete(promiseProcessLog.recordStats())
				.thenApply(result -> LogDiff.of(result.getValue1(), result.getValue2()))
				.whenResult(logDiff ->
						logger.info("Log '{}' processing complete. Positions: {}", log, logDiff.getPositions()));
	}

	private StreamSupplierWithResult<T, Map<String, LogPositionDiff>> getSupplier() {
		AsyncCollector<Map<String, LogPositionDiff>> logPositionsCollector = AsyncCollector.create(new HashMap<>());
		StreamUnion<T> streamUnion = StreamUnion.create();
		for (String partition : partitions) {
			String logName = logName(partition);
			LogPosition logPosition = state.getPositions().get(logName);
			if (logPosition == null) {
				logPosition = LogPosition.create(new LogFile("", 0), 0L);
			}
			logger.info("Starting reading '{}' from position {}", logName, logPosition);

			LogPosition logPositionFrom = logPosition;
			StreamSupplierWithResult<T, LogPosition> supplier = multilog.reader(partition, logPosition.getLogFile(), logPosition.getPosition(), null);
			supplier.getSupplier().streamTo(streamUnion.newInput());
			logPositionsCollector.addPromise(supplier.getResult(), (accumulator, logPositionTo) -> {
				if (!logPositionTo.equals(logPositionFrom)) {
					accumulator.put(logName, new LogPositionDiff(logPositionFrom, logPositionTo));
				}
			});
		}
		return StreamSupplierWithResult.of(
				streamUnion.getOutput()
						.transformWith(detailed ? streamStatsDetailed : streamStatsBasic),
				logPositionsCollector.run().get());
	}

	private String logName(String partition) {
		return log != null && !log.isEmpty() ? log + "." + partition : partition;
	}

	@JmxAttribute
	public boolean isEnabled() {
		return enabled;
	}

	@JmxAttribute
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	@JmxAttribute
	public PromiseStats getPromiseProcessLog() {
		return promiseProcessLog;
	}

	@JmxAttribute
	public PromiseStats getPromiseSupplier() {
		return promiseSupplier;
	}

	@JmxAttribute
	public PromiseStats getPromiseConsumer() {
		return promiseConsumer;
	}

	@JmxAttribute
	public StreamStatsBasic getStreamStatsBasic() {
		return streamStatsBasic;
	}

	@JmxAttribute
	public StreamStatsDetailed getStreamStatsDetailed() {
		return streamStatsDetailed;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailed = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailed = false;
	}
}
