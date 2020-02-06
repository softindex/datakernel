package io.datakernel.cube.service;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.function.AsyncPredicate;
import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.cube.Cube;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.etl.LogDiff;
import io.datakernel.etl.LogOTProcessor;
import io.datakernel.etl.LogOTState;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.eventloop.jmx.ValueStats;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.datakernel.async.function.AsyncSuppliers.coalesce;
import static io.datakernel.async.util.LogUtils.thisMethod;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.promise.Promises.asPromises;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class CubeLogProcessorController<K, C> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeLogProcessorController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final List<LogOTProcessor<?, CubeDiff>> logProcessors;
	private final AggregationChunkStorage<C> chunkStorage;
	private final OTStateManager<K, LogDiff<CubeDiff>> stateManager;
	private final AsyncPredicate<K> predicate;

	private boolean parallelRunner;

	private final PromiseStats promiseProcessLogs = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseProcessLogsImpl = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withRate();

	CubeLogProcessorController(Eventloop eventloop,
			List<LogOTProcessor<?, CubeDiff>> logProcessors,
			AggregationChunkStorage<C> chunkStorage,
			OTStateManager<K, LogDiff<CubeDiff>> stateManager,
			AsyncPredicate<K> predicate) {
		this.eventloop = eventloop;
		this.logProcessors = logProcessors;
		this.chunkStorage = chunkStorage;
		this.stateManager = stateManager;
		this.predicate = predicate;
	}

	public static <K, C> CubeLogProcessorController<K, C> create(Eventloop eventloop,
			LogOTState<CubeDiff> state,
			OTStateManager<K, LogDiff<CubeDiff>> stateManager,
			AggregationChunkStorage<C> chunkStorage,
			List<LogOTProcessor<?, CubeDiff>> logProcessors) {
		Cube cube = (Cube) state.getDataState();
		AsyncPredicate<K> predicate = AsyncPredicate.of(commitId -> {
			if (cube.containsExcessiveNumberOfOverlappingChunks()) {
				logger.info("Cube contains excessive number of overlapping chunks");
				return false;
			}
			return true;
		});
		return new CubeLogProcessorController<>(eventloop, logProcessors, chunkStorage, stateManager, predicate);
	}

	public CubeLogProcessorController<K, C> withParallelRunner(boolean parallelRunner) {
		this.parallelRunner = parallelRunner;
		return this;
	}

	private final AsyncSupplier<Boolean> processLogs = coalesce(this::doProcessLogs);

	public Promise<Boolean> processLogs() {
		return processLogs.get();
	}

	Promise<Boolean> doProcessLogs() {
		return process()
				.whenComplete(promiseProcessLogs.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	Promise<Boolean> process() {
		return Promise.complete()
				.then($ -> stateManager.sync())
				.map($ -> stateManager.getCommitId())
				.then(predicate::test)
				.then(ok -> {
					if (!ok) return Promise.of(false);

					logger.info("Pull to commit: {}, start log processing", stateManager.getCommitId());

					List<AsyncSupplier<LogDiff<CubeDiff>>> tasks = logProcessors.stream()
							.map(logProcessor -> AsyncSupplier.cast(logProcessor::processLog))
							.collect(toList());

					Promise<List<LogDiff<CubeDiff>>> promise = parallelRunner ?
							Promises.toList(tasks.stream().map(AsyncSupplier::get)) :
							Promises.reduce(toList(), 1, asPromises(tasks));

					return promise
							.whenComplete(promiseProcessLogsImpl.recordStats())
							.whenResult(this::cubeDiffJmx)
							.then(diffs -> Promise.complete()
									.whenResult(() -> stateManager.addAll(diffs))
									.then($ -> chunkStorage.finish(addedChunks(diffs)))
									.then($ -> stateManager.sync())
									.whenException(e -> stateManager.reset())
									.map($ -> true));
				})
				.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	private void cubeDiffJmx(List<LogDiff<CubeDiff>> logDiffs) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;

		for (LogDiff<CubeDiff> logDiff : logDiffs) {
			for (CubeDiff cubeDiff : logDiff.getDiffs()) {
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

	@SuppressWarnings("unchecked")
	private Set<C> addedChunks(List<LogDiff<CubeDiff>> diffs) {
		return diffs.stream()
				.flatMap(LogDiff::diffs)
				.flatMap(CubeDiff::addedChunks)
				.map(id -> (C) id)
				.collect(toSet());
	}

	@NotNull
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
	public PromiseStats getPromiseProcessLogs() {
		return promiseProcessLogs;
	}

	@JmxAttribute
	public PromiseStats getPromiseProcessLogsImpl() {
		return promiseProcessLogsImpl;
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
