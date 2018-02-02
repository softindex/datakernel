package io.datakernel.cube.service;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static io.datakernel.util.CollectionUtils.*;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public final class CubeCleanerController implements EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final long DEFAULT_CHUNKS_CLEANUP_DELAY = 60 * 1000L;
	public static final int DEFAULT_SNAPSHOTS_COUNT = 1;
	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;

	private final OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms;
	private final OTRemote<Integer, LogDiff<CubeDiff>> remote;
	private final LocalFsChunkStorage chunksStorage;

	private long freezeTimeout;

	private long chunksCleanupDelay = DEFAULT_CHUNKS_CLEANUP_DELAY;
	private int extraSnapshotsCount = DEFAULT_SNAPSHOTS_COUNT;

	private final ValueStats chunksCount = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupCollectRequiredChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupCheckRequiredChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupRemote = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeCleanerController(Eventloop eventloop,
	                      OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms,
	                      OTRemoteSql<LogDiff<CubeDiff>> remote,
	                      LocalFsChunkStorage chunksStorage) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
		this.remote = remote;
		this.chunksStorage = chunksStorage;
	}

	public static CubeCleanerController create(Eventloop eventloop,
	                                           OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms,
	                                           LocalFsChunkStorage storage) {
		return new CubeCleanerController(eventloop, algorithms, (OTRemoteSql<LogDiff<CubeDiff>>) algorithms.getRemote(), storage);
	}

	public CubeCleanerController withChunksCleanupDelay(long chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
		return this;
	}

	public CubeCleanerController withExtraSnapshotCount(int extraSnapshotCount) {
		this.extraSnapshotsCount = extraSnapshotCount;
		return this;
	}

	public CubeCleanerController withFreezeTimeout(long freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
		return this;
	}

	private static Set<Long> chunks(List<LogDiff<CubeDiff>> item) {
		return item.stream().flatMap(LogDiff::diffs)
				.flatMap(cubeDiff -> cubeDiff.keySet().stream().map(cubeDiff::get))
				.flatMap(diff -> concat(diff.getAddedChunks().stream(), diff.getRemovedChunks().stream()))
				.map(AggregationChunk::getChunkId)
				.collect(toSet());
	}

	private static Stream<LogDiff<CubeDiff>> commitToDiffs(OTCommit<Integer, LogDiff<CubeDiff>> commit) {
		return commit.getParents().values().stream().flatMap(Collection::stream);
	}

	private final AsyncCallable<Void> cleanup = sharedCall(this::doCleanup);

	public Stage<Void> cleanup() {
		return cleanup.call();
	}

	Stage<Void> doCleanup() {
		return remote.getHeads()
				.thenCompose(heads -> findFrozenCut(heads, eventloop.currentTimeMillis() - freezeTimeout))
				.thenCompose(this::cleanupFrozenCut)
				.whenComplete(stageCleanup.recordStats());
	}

	Stage<Set<Integer>> findFrozenCut(Set<Integer> heads, long freezeTimestamp) {
		return algorithms.findCut(heads,
				commits -> commits.stream().allMatch(commit -> commit.getTimestamp() < freezeTimestamp));
	}

	Stage<Void> cleanupFrozenCut(Set<Integer> frozenCut) {
		logger.info("Frozen cut: {}", frozenCut);
		return findBottomNodes(frozenCut)
				.thenCompose(bottomNodes -> bottomNodes.isPresent() ?
						algorithms.findFirstCommonParent(bottomNodes.get())
								.thenCompose(checkpointNode -> checkpointNode.isPresent() ?
										trySaveSnapshotAndCleanupChunks(checkpointNode.get()) :
										Stage.of(null)) :
						Stage.of(null));
	}

	Stage<Optional<Set<Integer>>> findBottomNodes(Set<Integer> parentCandidates) {
		return algorithms.findCommonParents(parentCandidates)
				.whenComplete(Stages.onResult(rootNodes -> logger.info("Root nodes: {}", rootNodes)))
				.thenApply(rootNodes -> rootNodes.isEmpty() ? Optional.empty() : Optional.of(rootNodes));
	}

	Stage<Void> trySaveSnapshotAndCleanupChunks(Integer checkpointNode) {
		logger.info("Checkpoint node: {}", checkpointNode);
		return algorithms.loadAllChanges(checkpointNode).thenCompose(changes -> {
			long cleanupTimestamp = eventloop.currentTimeMillis() - chunksCleanupDelay;

			return remote.saveSnapshot(checkpointNode, changes)
					.thenCompose($ -> findSnapshot(singleton(checkpointNode), extraSnapshotsCount))
					.thenCompose(lastSnapshot -> lastSnapshot.isPresent() ?
							remote.loadCommit(checkpointNode)
									.thenCompose(commit ->
											collectRequiredChunks(checkpointNode)
													.thenApply(extractedChunks -> union(chunks(changes), extractedChunks)))
									.thenCompose(chunks ->
											cleanup(lastSnapshot.get(), chunks, cleanupTimestamp)) :
							notEnoughSnapshots());
		});
	}

	Stage<Optional<Integer>> findSnapshot(Set<Integer> heads, int skipSnapshots) {
		return algorithms.findParent(heads,
				DiffsReducer.toVoid(),
				OTCommit::isSnapshot,
				null)
				.thenComposeAsync(findResult -> {
					if (!findResult.isFound()) return Stage.of(Optional.empty());
					if (skipSnapshots <= 0) return Stage.of(Optional.of(findResult.getCommit()));
					return findSnapshot(findResult.getCommitParents(), skipSnapshots - 1);
				});
	}

	private Stage<Void> notEnoughSnapshots() {
		logger.info("Not enough snapshots, skip cleanup");
		return Stage.of(null);
	}

	private Stage<Set<Long>> collectRequiredChunks(Integer checkpointNode) {
		return remote.getHeads()
				.thenCompose(heads -> algorithms.reduceEdges(heads,
						checkpointNode,
						DiffsReducer.of(new HashSet<>(),
								(Set<Long> accumulatedChunks, List<LogDiff<CubeDiff>> diffs) ->
										union(accumulatedChunks, chunks(diffs)))).whenComplete(stageCleanupCollectRequiredChunks.recordStats()))
				.thenApply(accumulators -> accumulators.values().stream().flatMap(Collection::stream).collect(toSet()));
	}

	private Stage<Void> cleanup(Integer checkpointNode, Set<Long> requiredChunks, long chunksCleanupTimestamp) {
		return checkRequiredChunks(requiredChunks)
				.thenCompose($ -> remote.cleanup(checkpointNode)
						.whenComplete(stageCleanupRemote.recordStats()))
				.thenCompose($ -> chunksStorage.cleanupBeforeTimestamp(requiredChunks, chunksCleanupTimestamp)
						.whenComplete(stageCleanupChunks.recordStats()));
	}

	private Stage<Void> checkRequiredChunks(Set<Long> requiredChunks) {
		return chunksStorage.list(s -> true, timestamp -> true)
				.whenComplete(Stages.onResult(chunks -> chunksCount.recordValue(chunks.size())))
				.thenCompose(actualChunks -> actualChunks.containsAll(requiredChunks) ?
						Stage.of((Void) null) :
						Stage.ofException(new IllegalStateException("Missed chunks from storage: " +
								toLimitedString(difference(requiredChunks, actualChunks), 100))))
				.whenComplete(stageCleanupCheckRequiredChunks.recordStats());
	}

	@JmxAttribute
	public long getChunksCleanupDelay() {
		return chunksCleanupDelay;
	}

	@JmxAttribute
	public void setChunksCleanupDelay(long chunksCleanupDelay) {
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
	public long getFreezeTimeout() {
		return freezeTimeout;
	}

	@JmxAttribute
	public void setFreezeTimeout(long freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
	}

	@JmxAttribute
	public ValueStats getChunksCount() {
		return chunksCount;
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
	public StageStats getStageCleanupCheckRequiredChunks() {
		return stageCleanupCheckRequiredChunks;
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
}
