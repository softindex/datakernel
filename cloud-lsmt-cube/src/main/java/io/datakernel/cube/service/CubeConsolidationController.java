package io.datakernel.cube.service;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.ot.OTStateManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toSet;

public final class CubeConsolidationController<K, D, C> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeConsolidationController.class);

	public static final Supplier<Function<Aggregation, Promise<AggregationDiff>>> DEFAULT_STRATEGY = new Supplier<Function<Aggregation,
			Promise<AggregationDiff>>>() {
		private boolean hotSegment = false;

		@Override
		public Function<Aggregation, Promise<AggregationDiff>> get() {
			return (hotSegment = !hotSegment) ?
					Aggregation::consolidateHotSegment :
					Aggregation::consolidateMinKey;
		}
	};
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final CubeDiffScheme<D> cubeDiffScheme;
	private final Cube cube;
	private final OTStateManager<K, D> stateManager;
	private final AggregationChunkStorage<C> aggregationChunkStorage;

	private final Supplier<Function<Aggregation, Promise<AggregationDiff>>> strategy;

	private final PromiseStats promiseConsolidate = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseConsolidateImpl = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final ValueStats removedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats removedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withRate();
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withRate();

	CubeConsolidationController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme, Cube cube,
			OTStateManager<K, D> stateManager,
			AggregationChunkStorage<C> aggregationChunkStorage,
			Supplier<Function<Aggregation, Promise<AggregationDiff>>> strategy) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.cube = cube;
		this.stateManager = stateManager;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.strategy = strategy;
	}

	public static <K, D, C> CubeConsolidationController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			Cube cube,
			OTStateManager<K, D> stateManager,
			AggregationChunkStorage<C> aggregationChunkStorage) {
		return new CubeConsolidationController<>(eventloop, cubeDiffScheme, cube, stateManager, aggregationChunkStorage, DEFAULT_STRATEGY);
	}

	public CubeConsolidationController<K, D, C> withStrategy(Supplier<Function<Aggregation, Promise<AggregationDiff>>> strategy) {
		return new CubeConsolidationController<>(eventloop, cubeDiffScheme, cube, stateManager, aggregationChunkStorage, strategy);
	}

	private final AsyncSupplier<Void> consolidate = reuse(this::doConsolidate);

	public Promise<Void> consolidate() {
		return consolidate.get();
	}

	Promise<Void> doConsolidate() {
		return Promise.complete()
				.then($ -> stateManager.sync())
				.then($ -> cube.consolidate(strategy.get()).whenComplete(promiseConsolidateImpl.recordStats()))
				.whenResult(this::cubeDiffJmx)
				.whenComplete(this::logCubeDiff)
				.then(cubeDiff -> {
					if (cubeDiff.isEmpty()) return Promise.complete();
					stateManager.add(cubeDiffScheme.wrap(cubeDiff));
					return Promise.complete()
							.then($ -> aggregationChunkStorage.finish(addedChunks(cubeDiff)))
							.then($ -> stateManager.sync())
							.whenException(e -> stateManager.reset())
							.whenComplete(toLogger(logger, thisMethod(), cubeDiff));
				})
				.whenComplete(promiseConsolidate.recordStats())
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

	@SuppressWarnings("unchecked")
	private static <C> Set<C> addedChunks(CubeDiff cubeDiff) {
		return cubeDiff.addedChunks().map(id -> (C) id).collect(toSet());
	}

	private void logCubeDiff(CubeDiff cubeDiff, Throwable e) {
		if (e != null) logger.warn("Consolidation failed", e);
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
	public PromiseStats getPromiseConsolidate() {
		return promiseConsolidate;
	}

	@JmxAttribute
	public PromiseStats getPromiseConsolidateImpl() {
		return promiseConsolidateImpl;
	}

	@JmxOperation
	public void consolidateNow() {
		consolidate();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
