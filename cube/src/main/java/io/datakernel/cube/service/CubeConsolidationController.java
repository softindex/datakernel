package io.datakernel.cube.service;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.AsyncFunction;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Stage;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.ot.OTStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toSet;

public final class CubeConsolidationController<K, D, C> implements EventloopJmxMBeanEx {
	public static final Supplier<AsyncFunction<Aggregation, AggregationDiff>> DEFAULT_STRATEGY = new Supplier<AsyncFunction<Aggregation, AggregationDiff>>() {
		private boolean hotSegment = false;

		@Override
		public AsyncFunction<Aggregation, AggregationDiff> get() {
			return (hotSegment = !hotSegment) ? Aggregation::consolidateHotSegment : Aggregation::consolidateMinKey;
		}
	};
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Logger logger = LoggerFactory.getLogger(CubeConsolidationController.class);
	private final Eventloop eventloop;
	private final CubeDiffScheme<D> cubeDiffScheme;
	private final Cube cube;
	private final OTStateManager<K, D> stateManager;
	private final AggregationChunkStorage<C> aggregationChunkStorage;

	private final Supplier<AsyncFunction<Aggregation, AggregationDiff>> strategy;

	private final StageStats stageConsolidate = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageConsolidateImpl = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final ValueStats removedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats removedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withRate();
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withRate();

	CubeConsolidationController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme, Cube cube,
			OTStateManager<K, D> stateManager,
			AggregationChunkStorage<C> aggregationChunkStorage,
			Supplier<AsyncFunction<Aggregation, AggregationDiff>> strategy) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.cube = cube;
		this.stateManager = stateManager;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.strategy = strategy;
	}

	public static <K, D, C> CubeConsolidationController create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			Cube cube,
			OTStateManager<K, D> stateManager,
			AggregationChunkStorage<C> aggregationChunkStorage) {
		return new CubeConsolidationController<>(eventloop, cubeDiffScheme, cube, stateManager, aggregationChunkStorage, DEFAULT_STRATEGY);
	}

	public CubeConsolidationController<K, D, C> withStrategy(Supplier<AsyncFunction<Aggregation, AggregationDiff>> strategy) {
		return new CubeConsolidationController<>(eventloop, cubeDiffScheme, cube, stateManager, aggregationChunkStorage, strategy);
	}

	private final AsyncSupplier<Void> consolidate = reuse(this::doConsolidate);

	public Stage<Void> consolidate() {
		return consolidate.get();
	}

	Stage<Void> doConsolidate() {
		return stateManager.pull()
				.thenCompose($ -> stateManager.getAlgorithms().mergeHeadsAndPush())
				.thenCompose(stateManager::pull)
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> cube.consolidate(strategy.get()).whenComplete(stageConsolidateImpl.recordStats()))
				.whenResult(this::cubeDiffJmx)
				.whenComplete(this::logCubeDiff)
				.thenCompose(this::tryPushConsolidation)
				.whenComplete(stageConsolidate.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	private void cubeDiffJmx(CubeDiff cubeDiff) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;
		long curRemovedChunks = 0;
		long curRemovedChunksRecords = 0;

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

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
		removedChunks.recordValue(curRemovedChunks);
		removedChunksRecords.recordValue(curRemovedChunksRecords);
	}

	private Stage<Void> tryPushConsolidation(CubeDiff cubeDiff) {
		if (cubeDiff.isEmpty()) return Stage.of(null);

		stateManager.add(cubeDiffScheme.wrap(cubeDiff));
		return stateManager.pull()
				.thenCompose($ -> stateManager.getAlgorithms().mergeHeadsAndPush())
				.thenCompose(stateManager::pull)
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> stateManager.commit())
				.thenCompose($ -> aggregationChunkStorage.finish(addedChunks(cubeDiff)))
				.thenCompose($ -> stateManager.push())
				.whenComplete(toLogger(logger, thisMethod(), cubeDiff));
	}

	@SuppressWarnings("unchecked")
	private static <C> Set<C> addedChunks(CubeDiff cubeDiff) {
		return cubeDiff.addedChunks().map(id -> (C) id).collect(toSet());
	}

	private void logCubeDiff(CubeDiff cubeDiff, Throwable throwable) {
		if (throwable != null) logger.warn("Consolidation failed", throwable);
		else if (cubeDiff.isEmpty()) logger.info("Previous consolidation did not merge any chunks");
		else logger.info("Consolidation finished. Launching consolidation task again.");
	}

	@JmxAttribute
	public ValueStats getRemovedChunks() {
		return removedChunks;
	}

	@JmxAttribute
	public ValueStats getAddedChunks() {
		return addedChunks;
	}

	@JmxAttribute
	public ValueStats getRemovedChunksRecords() {
		return removedChunksRecords;
	}

	@JmxAttribute
	public ValueStats getAddedChunksRecords() {
		return addedChunksRecords;
	}

	@JmxAttribute
	public StageStats getStageConsolidate() {
		return stageConsolidate;
	}

	@JmxAttribute
	public StageStats getStageConsolidateImpl() {
		return stageConsolidateImpl;
	}

	@JmxOperation
	public void consolidateNow() {
		consolidate();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
