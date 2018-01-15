package io.datakernel.cube.service;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stages;
import io.datakernel.cube.Cube;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.logfs.ot.LogOTProcessor;
import io.datakernel.logfs.ot.LogOTState;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.Stages.onResult;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class CubeLogProcessorController implements EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(CubeLogProcessorController.class);

	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;
	private final List<LogOTProcessor<?, CubeDiff>> logProcessors;
	private final OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms;
	private final OTSystem<LogDiff<CubeDiff>> otSystem;
	private final AggregationChunkStorage chunkStorage;
	private final OTStateManager<Integer, LogDiff<CubeDiff>> stateManager;
	private final LogOTState<CubeDiff> logOTState;
	private final Cube cube;

	private boolean parallelRunner;

	private final Stopwatch sw = Stopwatch.createUnstarted();
	private StageStats stageProcessLogs = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private StageStats stageProcessLogsImpl = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private ValueStats removedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private ValueStats removedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeLogProcessorController(Eventloop eventloop, List<LogOTProcessor<?, CubeDiff>> logProcessors, OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms, OTSystem<LogDiff<CubeDiff>> otSystem, AggregationChunkStorage chunkStorage, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager, LogOTState<CubeDiff> logOTState, Cube cube) {
		this.eventloop = eventloop;
		this.logProcessors = logProcessors;
		this.algorithms = algorithms;
		this.otSystem = otSystem;
		this.chunkStorage = chunkStorage;
		this.stateManager = stateManager;
		this.logOTState = logOTState;
		this.cube = cube;
	}

	public static CubeLogProcessorController create(Eventloop eventloop,
	                                                OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                                                AggregationChunkStorage chunkStorage,
	                                                List<LogOTProcessor<?, CubeDiff>> logProcessors) {
		OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms = stateManager.getAlgorithms();
		OTSystem<LogDiff<CubeDiff>> system = algorithms.getOtSystem();
		LogOTState<CubeDiff> logState = (LogOTState<CubeDiff>) stateManager.getState();
		Cube cube = (Cube) logState.getDataState();
		return new CubeLogProcessorController(eventloop, logProcessors, algorithms, system, chunkStorage, stateManager, logState, cube);
	}

	public CubeLogProcessorController withParallelRunner(boolean parallelRunner) {
		this.parallelRunner = parallelRunner;
		return this;
	}

	private final AsyncCallable<Boolean> processLogs = AsyncCallable.singleCallOf(this::doProcessLogs);

	public CompletionStage<Boolean> processLogs() {
		return processLogs.call();
	}

	CompletionStage<Boolean> doProcessLogs() {
		sw.reset().start();
		logger.info("Start log processing from head: {}", stateManager.getRevision());
		logger.trace("Start state: {}", stateManager);
		return stageProcessLogs.monitor(this::process)
				.whenComplete(this::logResult)
				.whenComplete(($, throwable) -> logger.trace("Finish state: {}", stateManager));
	}

	CompletionStage<Boolean> process() {
		return stateManager.pull()
				.thenCompose($_ -> {
					if (cube.containsExcessiveNumberOfOverlappingChunks()) {
						logger.info("Cube contains excessive number of overlapping chunks");
						return Stages.of(false);
					}

					logger.info("Pull to commit: {}, start log processing", stateManager.getRevision());

					List<AsyncCallable<LogDiff<CubeDiff>>> tasks = logProcessors.stream()
							.map(logProcessor -> AsyncCallable.of(logProcessor::processLog))
							.collect(toList());

					return stageProcessLogsImpl.monitor(
							parallelRunner ?
									Stages.collect(tasks.stream().map(AsyncCallable::call)) :
									Stages.collectSequence(tasks))
							.whenComplete(onResult(this::cubeDiffJmx))
							.thenCompose(diffs -> {
								stateManager.add(otSystem.squash(diffs));

								return stateManager.pull()
										.thenCompose($ -> stateManager.getAlgorithms().mergeHeadsAndPush())
										.thenCompose(stateManager::pull)
										.thenCompose($ -> stateManager.pull())
										.thenCompose($ -> stateManager.commit())
										.thenCompose($ -> chunkStorage.finish(addedChunks(diffs)))
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

		for (LogDiff<CubeDiff> logDiff : logDiffs) {
			for (CubeDiff cubeDiff : logDiff.diffs) {
				for (String key : cubeDiff.keySet()) {
					AggregationDiff aggregationDiff = cubeDiff.get(key);
					curAddedChunks += aggregationDiff.getAddedChunks().size();
					for (AggregationChunk aggregationChunk : aggregationDiff.getAddedChunks()) {
						curAddedChunksRecords += aggregationChunk.getCount();
					}

					curRemovedChunks += aggregationDiff.getRemovedChunks().size();
					for (AggregationChunk aggregationChunk : aggregationDiff.getRemovedChunks()) {
						curRemovedChunksRecords += aggregationChunk.getCount();
					}
				}
			}
		}

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
		removedChunks.recordValue(curRemovedChunks);
		removedChunksRecords.recordValue(curRemovedChunksRecords);
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
		return eventloop;
	}

	@JmxAttribute
	public ValueStats getLastRemovedChunks() {
		return removedChunks;
	}

	@JmxAttribute
	public ValueStats getLastAddedChunks() {
		return addedChunks;
	}

	@JmxAttribute
	public ValueStats getLastRemovedChunksRecords() {
		return removedChunksRecords;
	}

	@JmxAttribute
	public ValueStats getLastAddedChunksRecords() {
		return addedChunksRecords;
	}

	@JmxAttribute
	public StageStats getStageProcessLogs() {
		return stageProcessLogs;
	}

	@JmxAttribute
	public StageStats getStageProcessLogsImpl() {
		return stageProcessLogsImpl;
	}

	@JmxOperation
	public void processLogsNow() {
		processLogs();
	}

	@JmxOperation
	public void resetStats() {
		removedChunks.resetStats();
		removedChunksRecords.resetStats();
		addedChunks.resetStats();
		addedChunksRecords.resetStats();
		stageProcessLogs.resetStats();
		stageProcessLogsImpl.resetStats();
	}
}
