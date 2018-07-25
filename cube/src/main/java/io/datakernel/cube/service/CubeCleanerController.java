package io.datakernel.cube.service;

import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.ot.DiffsReducer;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteEx;
import io.datakernel.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.cube.Utils.chunksInDiffs;
import static io.datakernel.util.CollectionUtils.toLimitedString;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public final class CubeCleanerController<K, D, C> implements EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final Duration DEFAULT_CHUNKS_CLEANUP_DELAY = Duration.ofMinutes(1);
	public static final int DEFAULT_SNAPSHOTS_COUNT = 1;
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;

	private final OTAlgorithms<K, D> algorithms;
	private final OTRemoteEx<K, D> remote;
	private final RemoteFsChunkStorage<C> chunksStorage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private Duration freezeTimeout;

	private Duration chunksCleanupDelay = DEFAULT_CHUNKS_CLEANUP_DELAY;
	private int extraSnapshotsCount = DEFAULT_SNAPSHOTS_COUNT;

	private final StageStats stageCleanup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupCollectRequiredChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupRemote = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeCleanerController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			OTRemoteEx<K, D> remote,
			RemoteFsChunkStorage<C> chunksStorage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.algorithms = algorithms;
		this.remote = remote;
		this.chunksStorage = chunksStorage;
	}

	public static <K, D, C> CubeCleanerController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			RemoteFsChunkStorage<C> storage) {
		return new CubeCleanerController<>(eventloop, cubeDiffScheme, algorithms, (OTRemoteEx<K, D>) algorithms.getRemote(), storage);
	}

	public CubeCleanerController withChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
		return this;
	}

	public CubeCleanerController withExtraSnapshotCount(int extraSnapshotCount) {
		this.extraSnapshotsCount = extraSnapshotCount;
		return this;
	}

	public CubeCleanerController withFreezeTimeout(Duration freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
		return this;
	}

	private static <K, D> Stream<D> commitToDiffs(OTCommit<K, D> commit) {
		return commit.getParents().values().stream().flatMap(Collection::stream);
	}

	private final AsyncSupplier<Void> cleanup = reuse(this::doCleanup);

	public Stage<Void> cleanup() {
		return cleanup.get();
	}

	Stage<Void> doCleanup() {
		return remote.getHeads()
				.thenCompose(algorithms::excludeParents)
				.thenCompose(heads -> findFrozenCut(heads, eventloop.currentInstant().minus(freezeTimeout)))
				.thenCompose(this::cleanupFrozenCut)
				.whenComplete(stageCleanup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	Stage<Set<K>> findFrozenCut(Set<K> heads, Instant freezeTimestamp) {
		return algorithms.findCut(heads,
				commits -> commits.stream().allMatch(commit -> commit.getInstant().compareTo(freezeTimestamp) < 0))
				.whenComplete(toLogger(logger, thisMethod(), heads, freezeTimestamp));
	}

	Stage<Void> cleanupFrozenCut(Set<K> frozenCut) {
		return Stage.of(frozenCut)
				.thenCompose(algorithms::findAllCommonParents)
				.thenCompose(algorithms::findAnyCommonParent)
				.thenCompose(this::trySaveSnapshotAndCleanupChunks)
				.whenComplete(toLogger(logger, thisMethod(), frozenCut));
	}

	static class Tuple<K, D, C> {
		final Set<C> collectedChunks;
		final OTCommit<K, D> lastSnapshot;

		Tuple(Set<C> collectedChunks, OTCommit<K, D> lastSnapshot) {
			this.collectedChunks = collectedChunks;
			this.lastSnapshot = lastSnapshot;
		}
	}

	Stage<Void> trySaveSnapshotAndCleanupChunks(K checkpointNode) {
		return Stage.of(checkpointNode)
				.thenCompose(algorithms::checkout)
				.thenCompose(checkpointDiffs -> remote.saveSnapshot(checkpointNode, checkpointDiffs)
						.thenCompose($ -> findSnapshot(singleton(checkpointNode), extraSnapshotsCount))
						.thenCompose(lastSnapshot -> {
							if (lastSnapshot.isPresent())
								return Stages.toTuple(Tuple::new,
										collectRequiredChunks(checkpointNode),
										remote.loadCommit(lastSnapshot.get()))
										.thenCompose(tuple ->
												cleanup(lastSnapshot.get(),
														union(chunksInDiffs(cubeDiffScheme, checkpointDiffs), tuple.collectedChunks),
														tuple.lastSnapshot.getInstant().minus(chunksCleanupDelay)));
							else {
								logger.info("Not enough snapshots, skip cleanup");
								return Stage.of(null);
							}
						}))
				.whenComplete(toLogger(logger, thisMethod(), checkpointNode));
	}

	Stage<Optional<K>> findSnapshot(Set<K> heads, int skipSnapshots) {
		return algorithms.findParent(heads, DiffsReducer.toVoid(),
				commit -> commit.getSnapshotHint() == Boolean.FALSE ?
						Stage.of(false) :
						commit.getSnapshotHint() == Boolean.TRUE ?
								Stage.of(true) :
								remote.hasSnapshot(commit.getId()))
				.post()
				.thenCompose(findResult -> {
					if (!findResult.isFound()) return Stage.of(Optional.empty());
					else if (skipSnapshots <= 0) return Stage.of(Optional.of(findResult.getCommit()));
					else return findSnapshot(findResult.getCommitParents(), skipSnapshots - 1);
				});
	}

	private Stage<Set<C>> collectRequiredChunks(K checkpointNode) {
		return remote.getHeads()
				.thenCompose(heads ->
						algorithms.reduceEdges(heads, checkpointNode,
								DiffsReducer.of(
										new HashSet<>(),
										(Set<C> accumulatedChunks, List<D> diffs) ->
												union(accumulatedChunks, chunksInDiffs(cubeDiffScheme, diffs)),
										CollectionUtils::union))
								.whenComplete(stageCleanupCollectRequiredChunks.recordStats()))
				.thenApply(accumulators -> accumulators.values().stream().flatMap(Collection::stream).collect(toSet()))
				.whenComplete(transform(Set::size,
						toLogger(logger, thisMethod(), checkpointNode)));
	}

	private Stage<Void> cleanup(K checkpointNode, Set<C> requiredChunks, Instant chunksCleanupTimestamp) {
		return chunksStorage.checkRequiredChunks(requiredChunks)
				.thenCompose($ -> remote.cleanup(checkpointNode)
						.whenComplete(stageCleanupRemote.recordStats()))
				.thenCompose($ -> chunksStorage.cleanup(requiredChunks, chunksCleanupTimestamp)
						.whenComplete(stageCleanupChunks.recordStats()))
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), checkpointNode, chunksCleanupTimestamp, requiredChunks) :
						toLogger(logger, thisMethod(), checkpointNode, chunksCleanupTimestamp, toLimitedString(requiredChunks, 6)));
	}

	@JmxAttribute
	public Duration getChunksCleanupDelay() {
		return chunksCleanupDelay;
	}

	@JmxAttribute
	public void setChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
	}

	@JmxAttribute
	public int getExtraSnapshotsCount() {
		return extraSnapshotsCount;
	}

	@JmxAttribute
	public void setExtraSnapshotsCount(int extraSnapshotsCount) {
		this.extraSnapshotsCount = extraSnapshotsCount;
	}

	@JmxAttribute
	public Duration getFreezeTimeout() {
		return freezeTimeout;
	}

	@JmxAttribute
	public void setFreezeTimeout(Duration freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
	}

	@JmxAttribute
	public StageStats getStageCleanup() {
		return stageCleanup;
	}

	@JmxAttribute
	public StageStats getStageCleanupCollectRequiredChunks() {
		return stageCleanupCollectRequiredChunks;
	}

	@JmxAttribute
	public StageStats getStageCleanupRemote() {
		return stageCleanupRemote;
	}

	@JmxAttribute
	public StageStats getStageCleanupChunks() {
		return stageCleanupChunks;
	}

	@JmxOperation
	public void cleanupNow() {
		cleanup();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private static <T, R> BiConsumer<R, Throwable> transform(Function<? super R, ? extends T> fn, BiConsumer<? super T, Throwable> toConsumer) {
		return (value, throwable) -> toConsumer.accept(value != null ? fn.apply(value) : null, throwable);
	}
}
