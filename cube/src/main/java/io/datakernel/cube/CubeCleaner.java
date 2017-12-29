package io.datakernel.cube;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.Stages;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteSql;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static io.datakernel.ot.OTUtils.*;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.concat;

public class CubeCleaner {
	public static final int DEFAULT_SNAPSHOT_COUNT = 1;

	private final Logger logger = LoggerFactory.getLogger(CubeCleaner.class);
	private final Eventloop eventloop;
	private final OTRemoteSql<LogDiff<CubeDiff>> otRemote;
	private final Comparator<Integer> comparator;
	private final OTSystem<LogDiff<CubeDiff>> otSystem;
	private final LocalFsChunkStorage storage;
	private final long chunksCleanupDelay;
	private final int extraSnapshotCount;

	public CubeCleaner(Eventloop eventloop, OTRemoteSql<LogDiff<CubeDiff>> otRemote, Comparator<Integer> comparator,
	                   OTSystem<LogDiff<CubeDiff>> otSystem, LocalFsChunkStorage storage, long chunksCleanupDelay,
	                   int extraSnapshotCount) {
		this.eventloop = eventloop;
		this.otRemote = otRemote;
		this.comparator = comparator;
		this.otSystem = otSystem;
		this.storage = storage;
		this.chunksCleanupDelay = chunksCleanupDelay;
		this.extraSnapshotCount = extraSnapshotCount;
	}

	private static Stream<Long> chunks(Stream<LogDiff<CubeDiff>> item) {
		return item.flatMap(LogDiff::diffs)
				.flatMap(cubeDiff -> cubeDiff.keySet().stream().map(cubeDiff::get))
				.flatMap(diff -> concat(diff.getAddedChunks().stream(), diff.getRemovedChunks().stream()))
				.map(AggregationChunk::getChunkId);
	}

	private static Stream<LogDiff<CubeDiff>> commitToDiffs(OTCommit<Integer, LogDiff<CubeDiff>> commit) {
		return commit.getParents().values().stream().flatMap(Collection::stream);
	}

	public CompletionStage<Void> cleanup(Set<Integer> parentCandidates) {
		logger.info("Parent candidates: {}", parentCandidates);

		return findMergeNodes(parentCandidates).thenCompose(mergeNodesOpt -> mergeNodesOpt
				.map(mergeNodes -> findFirstCommonParent(otRemote, comparator, mergeNodes)
						.thenCompose(checkpointNode -> checkpointNode
								.map(this::trySaveSnapshotAndCleanupChunks)
								.orElse(Stages.of(null))))
				.orElse(Stages.of(null)));
	}

	private CompletionStage<Optional<Set<Integer>>> findMergeNodes(Set<Integer> parentCandidates) {
		return findCommonParents(otRemote, comparator, parentCandidates)
				.whenComplete(Stages.onResult(rootNodes -> logger.info("Root nodes: {}", rootNodes)))
				.thenApply(rootNodes -> rootNodes.isEmpty() ? Optional.empty() : Optional.of(rootNodes));
	}

	private CompletionStage<Void> trySaveSnapshotAndCleanupChunks(Integer checkpointNode) {
		logger.info("Checkpoint node: {}", checkpointNode);
		return loadAllChanges(otRemote, comparator, otSystem, checkpointNode).thenCompose(changes -> {
			final long cleanupTimestamp = eventloop.currentTimeMillis() - chunksCleanupDelay;

			return otRemote.saveSnapshot(checkpointNode, changes)
					.thenCompose($ -> findSnapshot(singleton(checkpointNode), extraSnapshotCount))
					.thenCompose(lastSnapshotOpt -> lastSnapshotOpt
							.map(lastSnapshot -> otRemote.loadCommit(checkpointNode)
									.thenCompose(commit -> extractChunks(checkpointNode)
											.thenApply(pathChunks -> Stream.of(
													chunks(commitToDiffs(commit)),
													chunks(changes.stream()),
													pathChunks.stream())
													.flatMap(longStream -> longStream)
													.collect(toSet())))
									.thenCompose(chunks -> cleanup(lastSnapshot, chunks, cleanupTimestamp)))
							.orElse(notEnoughSnapshots()));
		});
	}

	private CompletionStage<Optional<Integer>> findSnapshot(Set<Integer> heads, int skipSnapshots) {
		final Map<Integer, List<Object>> nodes = heads.stream().collect(toMap(o -> o, o -> Collections.emptyList()));
		return findParent(otRemote, comparator, nodes, (Integer) null,
				commit -> otRemote.isSnapshot(commit.getId()),
				(a, ds) -> Collections.emptyList())
				.thenComposeAsync(result -> {
					if (!result.isFound()) return Stages.of(Optional.empty());
					if (skipSnapshots <= 0) return Stages.of(Optional.of(result.getCommit()));
					return findSnapshot(result.getCommitParents(), skipSnapshots - 1);
				});
	}

	private CompletionStage<Void> notEnoughSnapshots() {
		logger.info("Not enough snapshots, skip cleanup");
		return Stages.of(null);
	}

	private CompletionStage<Set<Long>> extractChunks(Integer checkpointNode) {
		return otRemote.getHeads().thenCompose(heads -> addedChunksOnPath(checkpointNode, heads));
	}

	private CompletionStage<Set<Long>> addedChunksOnPath(Integer parent, Collection<Integer> heads) {
		final Map<Integer, Set<Long>> headsMap = heads.stream().collect(toMap(k -> k, k -> new HashSet<Long>()));
		return OTUtils.reduceEdges(otRemote, comparator, headsMap, parent,
				k -> k >= parent,
				(a, ds) -> concat(a.stream(), chunks(ds.stream())).collect(toSet()),
				(longs, longs2) -> concat(longs.stream(), longs2.stream()).collect(toSet()))
				.thenApply(accumulators -> accumulators.values().stream()
						.flatMap(Collection::stream)
						.collect(toSet()));
	}

	private CompletionStage<Void> cleanup(Integer checkpointNode, Set<Long> saveChunks, long chunksCleanupTimestamp) {
		return compareChunks(saveChunks)
				.thenCompose($ -> otRemote.cleanup(checkpointNode))
				.thenCompose($ -> storage.cleanupBeforeTimestamp(saveChunks, chunksCleanupTimestamp));
	}

	private CompletionStage<Void> compareChunks(Set<Long> cubeChunks) {
		return storage.list(s -> true, timestamp -> true)
				.thenCompose(storageChunks -> storageChunks.containsAll(cubeChunks)
						? Stages.of(null)
						: Stages.<Void>ofException(new IllegalStateException("Missed chunks from storage: " +
						cubeChunks.stream().filter(v -> !storageChunks.contains(v)).collect(toList()))));
	}

}
