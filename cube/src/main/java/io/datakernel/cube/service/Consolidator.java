package io.datakernel.cube.service;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.Stages;
import io.datakernel.cube.Cube;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTStateManager;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.util.stream.Collectors.toSet;

public final class Consolidator implements EventloopJmxMBean {
	public static final Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> DEFAULT_STRATEGY = new Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>>() {
		private boolean hotSegment = false;

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Function<Aggregation, CompletionStage<AggregationDiff>> next() {
			return (hotSegment = !hotSegment) ? Aggregation::consolidateHotSegment : Aggregation::consolidateMinKey;
		}
	};

	private final Logger logger = LoggerFactory.getLogger(Consolidator.class);
	private final Eventloop eventloop;
	private final Cube cube;
	private final OTStateManager<Integer, LogDiff<CubeDiff>> stateManager;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final Stopwatch sw = Stopwatch.createUnstarted();

	private final Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> strategy;

	private long lastRemovedChunks = 0;
	private long lastRemovedChunksRecords = 0;
	private long lastAddedChunks = 0;
	private long lastAddedChunksRecords = 0;

	private Consolidator(Eventloop eventloop, Cube cube, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                     AggregationChunkStorage aggregationChunkStorage,
	                     Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> strategy) {
		this.eventloop = eventloop;
		this.cube = cube;
		this.stateManager = stateManager;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.strategy = strategy;
	}

	public static Consolidator create(Eventloop eventloop, Cube cube, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                                  AggregationChunkStorage aggregationChunkStorage) {
		return new Consolidator(eventloop, cube, stateManager, aggregationChunkStorage, DEFAULT_STRATEGY);
	}

	public Consolidator withStrategy(Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> strategy) {
		return new Consolidator(eventloop, cube, stateManager, aggregationChunkStorage, strategy);
	}

	public CompletionStage<Void> consolidate() {
		sw.reset().start();
		return stateManager.pull()
				.thenCompose($ -> stateManager.mergeHeadsAndPush())
				.thenCompose(stateManager::pull)
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> cube.consolidate(strategy.next()))
				.whenComplete(Stages.onResult(this::cubeDiffJmx))
				.whenComplete(this::logCubeDiff)
				.thenCompose(this::tryPushConsolidation)
				.whenComplete(this::logResult)
				.thenApply($ -> null);

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

		lastAddedChunks = curAddedChunks;
		lastAddedChunksRecords = curAddedChunksRecords;
		lastRemovedChunks = curRemovedChunks;
		lastRemovedChunksRecords = curRemovedChunksRecords;
	}

	private CompletionStage<Void> tryPushConsolidation(CubeDiff cubeDiff) {
		if (cubeDiff.isEmpty()) return Stages.of(null);

		stateManager.add(LogDiff.forCurrentPosition(cubeDiff));
		return stateManager.pull()
				.thenCompose($ -> stateManager.mergeHeadsAndPush())
				.thenCompose(stateManager::pull)
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> stateManager.commit())
				.thenCompose($ -> stateManager.push())
				.thenCompose($ -> aggregationChunkStorage.finish(addedChunks(cubeDiff)))
				;

	}

	private static Set<Long> addedChunks(CubeDiff cubeDiff) {
		return cubeDiff.addedChunks().collect(toSet());
	}

	private void logCubeDiff(CubeDiff cubeDiff, Throwable throwable) {
		if (throwable != null) logger.warn("Consolidation failed", throwable);
		else if (cubeDiff.isEmpty()) logger.info("Previous consolidation did not merge any chunks");
		else logger.info("Consolidation finished. Launching consolidation task again.");
	}

	private <T> void logResult(T value, Throwable throwable) {
		if (throwable == null) logger.info("Consolidator finish in {}", sw.stop());
		else logger.error("Consolidator error in {}", sw.stop(), throwable);
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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
