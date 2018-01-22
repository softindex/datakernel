package io.datakernel.cube.service;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncPredicate;
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

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.async.Stages.onResult;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class CubeLogProcessorController implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeLogProcessorController.class);

	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;
	private final List<LogOTProcessor<?, CubeDiff>> logProcessors;
	private final OTSystem<LogDiff<CubeDiff>> otSystem;
	private final AggregationChunkStorage chunkStorage;
	private final OTStateManager<Integer, LogDiff<CubeDiff>> stateManager;
	private final AsyncPredicate<Integer> predicate;

	private boolean parallelRunner;

	private final Stopwatch sw = Stopwatch.createUnstarted();
	private StageStats stageProcessLogs = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private StageStats stageProcessLogsImpl = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeLogProcessorController(Eventloop eventloop, List<LogOTProcessor<?, CubeDiff>> logProcessors, OTSystem<LogDiff<CubeDiff>> otSystem, AggregationChunkStorage chunkStorage, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager, AsyncPredicate<Integer> predicate) {
		this.eventloop = eventloop;
		this.logProcessors = logProcessors;
		this.otSystem = otSystem;
		this.chunkStorage = chunkStorage;
		this.stateManager = stateManager;
		this.predicate = predicate;
	}

	public static CubeLogProcessorController create(Eventloop eventloop,
	                                                OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                                                AggregationChunkStorage chunkStorage,
	                                                List<LogOTProcessor<?, CubeDiff>> logProcessors) {
		OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms = stateManager.getAlgorithms();
		OTSystem<LogDiff<CubeDiff>> system = algorithms.getOtSystem();
		LogOTState<CubeDiff> logState = (LogOTState<CubeDiff>) stateManager.getState();
		Cube cube = (Cube) logState.getDataState();
		AsyncPredicate<Integer> predicate = AsyncPredicate.of(commitId -> {
			if (cube.containsExcessiveNumberOfOverlappingChunks()) {
				logger.info("Cube contains excessive number of overlapping chunks");
				return false;
			}
			return true;
		});
		return new CubeLogProcessorController(eventloop, logProcessors, system, chunkStorage, stateManager, predicate);
	}

	public CubeLogProcessorController withParallelRunner(boolean parallelRunner) {
		this.parallelRunner = parallelRunner;
		return this;
	}

	private final AsyncCallable<Boolean> processLogs = sharedCall(this::doProcessLogs);

	public CompletionStage<Boolean> processLogs() {
		return processLogs.call();
	}

	CompletionStage<Boolean> doProcessLogs() {
		sw.reset().start();
		logger.info("Start log processing from head: {}", stateManager.getRevision());
		logger.trace("Start state: {}", stateManager);
		return process()
				.whenComplete(stageProcessLogs.recordStats())
				.whenComplete(this::logResult)
				.whenComplete(($, throwable) -> logger.trace("Finish state: {}", stateManager));
	}

	CompletionStage<Boolean> process() {
		return stateManager.pull()
				.thenCompose(predicate::test)
				.thenCompose(ok -> {
					if (!ok) return Stages.of(false);

					logger.info("Pull to commit: {}, start log processing", stateManager.getRevision());

					List<AsyncCallable<LogDiff<CubeDiff>>> tasks = logProcessors.stream()
							.map(logProcessor -> AsyncCallable.of(logProcessor::processLog))
							.collect(toList());

					CompletionStage<List<LogDiff<CubeDiff>>> stage = parallelRunner ?
							Stages.collect(tasks.stream().map(AsyncCallable::call)) :
							Stages.collectSequence(tasks);

					return stage
							.whenComplete(stageProcessLogsImpl.recordStats())
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

		for (LogDiff<CubeDiff> logDiff : logDiffs) {
			for (CubeDiff cubeDiff : logDiff.diffs) {
				for (String key : cubeDiff.keySet()) {
					AggregationDiff aggregationDiff = cubeDiff.get(key);
					curAddedChunks += aggregationDiff.getAddedChunks().size();
					for (AggregationChunk aggregationChunk : aggregationDiff.getAddedChunks()) {
						curAddedChunksRecords += aggregationChunk.getCount();
					}
				}
			}
		}

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
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
	public ValueStats getLastAddedChunks() {
		return addedChunks;
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

	@JmxAttribute
	public boolean isParallelRunner() {
		return parallelRunner;
	}

	@JmxAttribute
	public void setParallelRunner(boolean parallelRunner) {
		this.parallelRunner = parallelRunner;
	}

	@JmxOperation
	public void processLogsNow() {
		processLogs();
	}

}
