package io.datakernel.cube.service;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncFunction;
import io.datakernel.async.Stages;
import io.datakernel.cube.Cube;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.logfs.ot.LogOTProcessor;
import io.datakernel.logfs.ot.LogOTState;
import io.datakernel.ot.OTRemote;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class LogProcessorManager implements EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(LogProcessorManager.class);

	private final OTStateManager<Integer, LogDiff<CubeDiff>> stateManager;
	private final AggregationChunkStorage storage;
	private final OTSystem<LogDiff<CubeDiff>> otSystem;
	private final List<LogOTProcessor<?, CubeDiff>> processors;
	private final Cube cube;
	private final LogOTState<CubeDiff> logOTState;
	private final Stopwatch sw = Stopwatch.createUnstarted();

	private final AsyncFunction<List<AsyncCallable<LogDiff<CubeDiff>>>, List<LogDiff<CubeDiff>>> runner;
	private final OTRemote<Integer, LogDiff<CubeDiff>> otRemote;
	private final Comparator<Integer> comparator;

	private long lastRemovedChunks = 0;
	private long lastRemovedChunksRecords = 0;
	private long lastAddedChunks = 0;
	private long lastAddedChunksRecords = 0;

	private LogProcessorManager(OTStateManager<Integer, LogDiff<CubeDiff>> stateManager, Cube cube,
	                            List<LogOTProcessor<?, CubeDiff>> processors, AggregationChunkStorage storage,
	                            OTSystem<LogDiff<CubeDiff>> otSystem,
	                            LogOTState<CubeDiff> logOTState,
	                            AsyncFunction<List<AsyncCallable<LogDiff<CubeDiff>>>, List<LogDiff<CubeDiff>>> runner,
	                            OTRemote<Integer, LogDiff<CubeDiff>> otRemote, Comparator<Integer> comparator) {
		this.storage = storage;
		this.processors = processors;
		this.stateManager = stateManager;
		this.otSystem = otSystem;
		this.cube = cube;
		this.logOTState = logOTState;
		this.runner = runner;
		this.otRemote = otRemote;
		this.comparator = comparator;
	}

	public static LogProcessorManager create(OTStateManager<Integer, LogDiff<CubeDiff>> stateManager, Cube cube,
	                                         List<LogOTProcessor<?, CubeDiff>> processors, AggregationChunkStorage storage,
	                                         OTSystem<LogDiff<CubeDiff>> otSystem,
	                                         OTRemote<Integer, LogDiff<CubeDiff>> otRemote,
	                                         Comparator<Integer> comparator, LogOTState<CubeDiff> logOTState) {
		return new LogProcessorManager(stateManager, cube, processors, storage, otSystem, logOTState, Stages::collectSequence, otRemote, comparator);
	}

	public LogProcessorManager withParallelRunner(boolean parallelRunner) {
		return !parallelRunner ? this
				: new LogProcessorManager(stateManager, cube, processors, storage, otSystem, logOTState,
				Stages::collectCallable, otRemote, comparator);
	}

	public CompletionStage<Boolean> processLogs() {
		sw.reset().start();
		logger.info("Start log processing from head: {}", stateManager.getRevision());
		logger.trace("Start state: {}", stateManager);
		return process()
				.whenComplete(this::logResult)
				.whenComplete(($, throwable) -> logger.trace("Finish state: {}", stateManager));
	}

	private CompletionStage<Boolean> process() {
		return stateManager.pull().thenCompose(aVoid -> {
			if (cube.containsExcessiveNumberOfOverlappingChunks()) {
				logger.info("Cube contains excessive number of overlapping chunks");
				return Stages.of(false);
			}

			logger.info("Pull to commit: {}, start log processing", stateManager.getRevision());

			final List<AsyncCallable<LogDiff<CubeDiff>>> tasks = processors.stream()
					.map(logProcessor -> AsyncCallable.of(logProcessor::processLog))
					.collect(toList());

			return runner.apply(tasks)
					.whenComplete(Stages.onResult(this::cubeDiffJmx))
					.thenCompose(diffs -> {
						stateManager.add(otSystem.squash(diffs));

						return stateManager.pull()
								.thenCompose($ -> stateManager.mergeHeadsAndPush())
								.thenCompose(stateManager::pull)
								.thenCompose($ -> stateManager.pull())
								.thenCompose($ -> stateManager.commit())
								.thenCompose($ -> storage.finish(addedChunks(diffs)))
								.thenCompose($ -> stateManager.push())
								.thenApply($ -> true);
					});
		});
	}

	private void cubeDiffJmx(List<LogDiff<CubeDiff>> logDiffs) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;
		long curRemovedChunks = 0;
		long curRemovedChunksRecords = 0;

		for (final LogDiff<CubeDiff> logDiff : logDiffs) {
			for (final CubeDiff cubeDiff : logDiff.diffs) {
				for (final String key : cubeDiff.keySet()) {
					final AggregationDiff aggregationDiff = cubeDiff.get(key);
					curAddedChunks += aggregationDiff.getAddedChunks().size();
					for (final AggregationChunk aggregationChunk : aggregationDiff.getAddedChunks()) {
						curAddedChunksRecords += aggregationChunk.getCount();
					}

					curRemovedChunks += aggregationDiff.getRemovedChunks().size();
					for (final AggregationChunk aggregationChunk : aggregationDiff.getRemovedChunks()) {
						curRemovedChunksRecords += aggregationChunk.getCount();
					}
				}
			}
		}

		lastAddedChunks = curAddedChunks;
		lastAddedChunksRecords = curAddedChunksRecords;
		lastRemovedChunks = curRemovedChunks;
		lastRemovedChunksRecords = curRemovedChunksRecords;
	}

	private Set<Long> addedChunks(List<LogDiff<CubeDiff>> diffs) {
		return diffs.stream()
				.flatMap(LogDiff::diffs)
				.flatMap(CubeDiff::addedChunks)
				.collect(toSet());
	}

	private <T> void logResult(T value, Throwable throwable) {
		if (throwable == null) logger.info("Processor finish in {}", sw.stop());
		else logger.error("Processor error in {}", sw.stop(), throwable);
	}

	@Override
	public Eventloop getEventloop() {
		return cube.getEventloop();
	}

	@JmxAttribute
	public long getLastRemovedChunks() {
		return lastRemovedChunks;
	}

	@JmxAttribute
	public long getLastAddedChunks() {
		return lastAddedChunks;
	}

	@JmxAttribute
	public long getLastRemovedChunksRecords() {
		return lastRemovedChunksRecords;
	}

	@JmxAttribute
	public long getLastAddedChunksRecords() {
		return lastAddedChunksRecords;
	}

	@JmxOperation
	public void resetStats() {
		lastRemovedChunks = 0;
		lastRemovedChunksRecords = 0;
		lastAddedChunks = 0;
		lastAddedChunksRecords = 0;
	}
}
