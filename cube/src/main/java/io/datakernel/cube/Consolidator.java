package io.datakernel.cube;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.Stages;
import io.datakernel.cube.ot.CubeDiff;
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

public final class Consolidator {
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
	private final Cube cube;
	private final OTStateManager<Integer, LogDiff<CubeDiff>> stateManager;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final Stopwatch sw = Stopwatch.createUnstarted();

	private final Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> strategy;

	private Consolidator(Cube cube, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                     AggregationChunkStorage aggregationChunkStorage,
	                     Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> strategy) {
		this.cube = cube;
		this.stateManager = stateManager;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.strategy = strategy;
	}

	public static Consolidator create(Cube cube, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                                  AggregationChunkStorage aggregationChunkStorage) {
		return new Consolidator(cube, stateManager, aggregationChunkStorage, DEFAULT_STRATEGY);
	}

	public Consolidator withStrategy(Iterator<Function<Aggregation, CompletionStage<AggregationDiff>>> strategy) {
		return new Consolidator(cube, stateManager, aggregationChunkStorage, strategy);
	}

	public CompletionStage<Void> consolidate() {
		sw.reset().start();
		return stateManager.pull()
				.thenCompose($ -> stateManager.mergeHeadsAndPush())
				.thenCompose(stateManager::pull)
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> cube.consolidate(strategy.next()))
				.whenComplete(this::logCubeDiff)
				.thenCompose(this::tryPushConsolidation)
				.whenComplete(this::logResult)
				.thenApply($ -> null);
	}

	private CompletionStage<Void> tryPushConsolidation(CubeDiff cubeDiff) {
		if (cubeDiff.isEmpty()) return Stages.of(null);

		stateManager.add(LogDiff.forCurrentPosition(cubeDiff));
		return stateManager.pull()
				.thenCompose($ -> stateManager.mergeHeadsAndPush())
				.thenCompose(stateManager::pull)
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> stateManager.commit())
				.thenCompose($ -> aggregationChunkStorage.finish(addedChunks(cubeDiff)))
				.thenCompose($ -> stateManager.push());

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
}
