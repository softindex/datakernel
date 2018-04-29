package io.datakernel.cube.service;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.*;
import io.datakernel.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.async.StageConsumer.transform;
import static io.datakernel.async.Stages.collectToList;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public final class CubeCleanerController implements EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final Duration DEFAULT_CHUNKS_CLEANUP_DELAY = Duration.ofMinutes(1);
	public static final int DEFAULT_SNAPSHOTS_COUNT = 1;
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;

	private final OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms;
	private final OTRemote<Integer, LogDiff<CubeDiff>> remote;
	private final LocalFsChunkStorage chunksStorage;

	private Duration freezeTimeout;

	private Duration chunksCleanupDelay = DEFAULT_CHUNKS_CLEANUP_DELAY;
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
				.thenCompose(heads -> findFrozenCut(heads, eventloop.currentTimeMillis() - freezeTimeout.toMillis()))
				.thenCompose(this::cleanupFrozenCut)
				.whenComplete(stageCleanup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	Stage<Set<Integer>> findFrozenCut(Set<Integer> heads, long freezeTimestamp) {
		return algorithms.findCut(heads,
				commits -> commits.stream().allMatch(commit -> commit.getTimestamp() < freezeTimestamp))
				.whenComplete(toLogger(logger, thisMethod(), heads, freezeTimestamp));
	}

	Stage<Void> cleanupFrozenCut(Set<Integer> frozenCut) {
		logger.info("Frozen cut: {}", frozenCut);
		return findBottomNodes(frozenCut)
				.thenCompose(bottomNodes -> bottomNodes.isPresent() ?
						algorithms.findFirstCommonParent(bottomNodes.get())
								.thenCompose(checkpointNode -> checkpointNode.isPresent() ?
										trySaveSnapshotAndCleanupChunks(checkpointNode.get()) :
										Stage.of(null)) :
						Stage.of(null))
				.whenComplete(toLogger(logger, thisMethod(), frozenCut));
	}

	Stage<Optional<Set<Integer>>> findBottomNodes(Set<Integer> parentCandidates) {
		return algorithms.findCommonParents(parentCandidates)
				.thenApply(rootNodes -> rootNodes.isEmpty() ? Optional.<Set<Integer>>empty() : Optional.of(rootNodes))
				.whenComplete(toLogger(logger, thisMethod(), parentCandidates));
	}

	Stage<Void> trySaveSnapshotAndCleanupChunks(Integer checkpointNode) {
		return algorithms.checkout(checkpointNode)
				.thenCompose(checkpointDiffs -> {
					long cleanupTimestamp = eventloop.currentTimeMillis();

					return remote.saveSnapshot(checkpointNode, checkpointDiffs)
							.thenCompose($ -> {
								SettableStage<Optional<Integer>> result = SettableStage.create();
								findSnapshot(singleton(checkpointNode), extraSnapshotsCount, result);
								return result;
							})
							.thenCompose(lastSnapshot -> lastSnapshot.isPresent() ?
									collectRequiredChunks(checkpointNode)
											.thenApply(extractedChunks -> union(chunks(checkpointDiffs), extractedChunks))
											.thenCompose(chunks ->
													remote.getHeads()
															.thenCompose(heads -> collectToList(heads.stream().map(remote::loadCommit)))
															.thenApply(headCommits -> Math.min(cleanupTimestamp,
																	headCommits.stream()
																			.mapToLong(OTCommit::getTimestamp)
																			.min()
																			.getAsLong()) - chunksCleanupDelay.toMillis())
															.thenCompose(timestamp -> cleanup(lastSnapshot.get(), chunks, timestamp))
											) :
									notEnoughSnapshots());
				})
				.whenComplete(toLogger(logger, thisMethod(), checkpointNode));
	}

	void findSnapshot(Set<Integer> heads, int skipSnapshots, Callback<Optional<Integer>> cb) {
		algorithms.findParent(heads, DiffsReducer.toVoid(), OTCommit::isSnapshot, null)
				.whenResult(findResult -> {
					if (!findResult.isFound()) cb.set(Optional.empty());
					else if (skipSnapshots <= 0) cb.set(Optional.of(findResult.getCommit()));
					else findSnapshot(findResult.getCommitParents(), skipSnapshots - 1, cb);
				})
				.whenException(cb::setException);
	}

	private Stage<Void> notEnoughSnapshots() {
		logger.info("Not enough snapshots, skip cleanup");
		return Stage.of(null);
	}

	private Stage<Set<Long>> collectRequiredChunks(Integer checkpointNode) {
		return remote.getHeads()
				.thenCompose(heads -> algorithms.reduceEdges(heads, checkpointNode,
						DiffsReducer.of(
								new HashSet<>(),
								(Set<Long> accumulatedChunks, List<LogDiff<CubeDiff>> diffs) ->
										union(accumulatedChunks, chunks(diffs)),
								CollectionUtils::union))
						.whenComplete(stageCleanupCollectRequiredChunks.recordStats()))
				.thenApply(accumulators -> accumulators.values().stream().flatMap(Collection::stream).collect(toSet()))
				.whenComplete(transform(Set::size,
						toLogger(logger, thisMethod(), checkpointNode)));
	}

	private Stage<Void> cleanup(Integer checkpointNode, Set<Long> requiredChunks, long chunksCleanupTimestamp) {
		return checkRequiredChunks(requiredChunks)
				.thenCompose($ -> remote.cleanup(checkpointNode)
						.whenComplete(stageCleanupRemote.recordStats()))
				.thenCompose($ -> chunksStorage.cleanupBeforeTimestamp(requiredChunks, chunksCleanupTimestamp)
						.whenComplete(stageCleanupChunks.recordStats()))
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), checkpointNode, chunksCleanupTimestamp, requiredChunks) :
						toLogger(logger, thisMethod(), checkpointNode, chunksCleanupTimestamp, toLimitedString(requiredChunks, 6)));
	}

	private Stage<Void> checkRequiredChunks(Set<Long> requiredChunks) {
		return chunksStorage.list(s -> true, timestamp -> true)
				.whenResult(actualChunks -> chunksCount.recordValue(actualChunks.size()))
				.thenCompose(actualChunks -> actualChunks.containsAll(requiredChunks) ?
						Stage.of((Void) null) :
						Stage.ofException(new IllegalStateException("Missed chunks from storage: " +
								toLimitedString(difference(requiredChunks, actualChunks), 100))))
				.whenComplete(stageCleanupCheckRequiredChunks.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), toLimitedString(requiredChunks, 6)));
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
