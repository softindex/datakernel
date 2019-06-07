package io.datakernel.cube.service;

import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.async.*;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.logger.LoggerFactory;
import io.datakernel.ot.DiffsReducer;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepositoryEx;
import io.datakernel.ot.OTSystem;
import io.datakernel.util.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.cube.Utils.chunksInDiffs;
import static io.datakernel.ot.OTAlgorithms.*;
import static io.datakernel.util.CollectionUtils.toLimitedString;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public final class CubeCleanerController<K, D, C> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class.getName());

	public static final Duration DEFAULT_CHUNKS_CLEANUP_DELAY = Duration.ofMinutes(1);
	public static final int DEFAULT_SNAPSHOTS_COUNT = 1;
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;

	private final OTSystem<D> otSystem;
	private final OTRepositoryEx<K, D> repository;
	private final RemoteFsChunkStorage<C> chunksStorage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private Duration freezeTimeout;

	private Duration chunksCleanupDelay = DEFAULT_CHUNKS_CLEANUP_DELAY;
	private int extraSnapshotsCount = DEFAULT_SNAPSHOTS_COUNT;

	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupCollectRequiredChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupRepository = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeCleanerController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTRepositoryEx<K, D> repository,
			OTSystem<D> otSystem,
			RemoteFsChunkStorage<C> chunksStorage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.otSystem = otSystem;
		this.repository = repository;
		this.chunksStorage = chunksStorage;
	}

	public static <K, D, C> CubeCleanerController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTRepositoryEx<K, D> repository,
			OTSystem<D> otSystem,
			RemoteFsChunkStorage<C> storage) {
		return new CubeCleanerController<>(eventloop, cubeDiffScheme, repository, otSystem, storage);
	}

	public CubeCleanerController<K, D, C> withChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
		return this;
	}

	public CubeCleanerController<K, D, C> withExtraSnapshotsCount(int extraSnapshotsCount) {
		this.extraSnapshotsCount = extraSnapshotsCount;
		return this;
	}

	public CubeCleanerController<K, D, C> withFreezeTimeout(Duration freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
		return this;
	}

	private static <K, D> Stream<D> commitToDiffs(OTCommit<K, D> commit) {
		return commit.getParents().values().stream().flatMap(Collection::stream);
	}

	private final AsyncSupplier<Void> cleanup = reuse(this::doCleanup);

	public Promise<Void> cleanup() {
		return cleanup.get();
	}

	Promise<Void> doCleanup() {
		return repository.getHeads()
				.then(heads -> excludeParents(repository, otSystem, heads))
				.then(heads -> findFrozenCut(heads, eventloop.currentInstant().minus(freezeTimeout)))
				.then(this::cleanupFrozenCut)
				.thenEx((v, e) -> {
					if (e == GRAPH_EXHAUSTED) return Promise.of(null);
					return Promise.of(v, e);
				})
				.whenComplete(promiseCleanup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	Promise<Set<K>> findFrozenCut(Set<K> heads, Instant freezeTimestamp) {
		return findCut(repository, otSystem, heads,
				commits -> commits.stream().allMatch(commit -> commit.getInstant().compareTo(freezeTimestamp) < 0))
				.whenComplete(toLogger(logger, thisMethod(), heads, freezeTimestamp));
	}

	Promise<Void> cleanupFrozenCut(Set<K> frozenCut) {
		return findAllCommonParents(repository, otSystem, frozenCut)
				.then(parents -> findAnyCommonParent(repository, otSystem, parents))
				.then(this::trySaveSnapshotAndCleanupChunks)
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

	Promise<Void> trySaveSnapshotAndCleanupChunks(K checkpointNode) {
		return checkout(repository, otSystem, checkpointNode)
				.then(checkpointDiffs -> repository.saveSnapshot(checkpointNode, checkpointDiffs)
						.then($ -> findSnapshot(singleton(checkpointNode), extraSnapshotsCount))
						.then(lastSnapshot -> {
							if (lastSnapshot.isPresent())
								return Promises.toTuple(Tuple::new,
										collectRequiredChunks(checkpointNode),
										repository.loadCommit(lastSnapshot.get()))
										.then(tuple ->
												cleanup(lastSnapshot.get(),
														union(chunksInDiffs(cubeDiffScheme, checkpointDiffs), tuple.collectedChunks),
														tuple.lastSnapshot.getInstant().minus(chunksCleanupDelay)));
							else {
								logger.info("Not enough snapshots, skip cleanup");
								return Promise.complete();
							}
						}))
				.whenComplete(toLogger(logger, thisMethod(), checkpointNode));
	}

	Promise<Optional<K>> findSnapshot(Set<K> heads, int skipSnapshots) {
		return Promise.ofCallback(cb -> findSnapshotImpl(heads, skipSnapshots, cb));
	}

	private void findSnapshotImpl(Set<K> heads, int skipSnapshots, SettableCallback<Optional<K>> cb) {
		findParent(repository, otSystem, heads, DiffsReducer.toVoid(),
				commit -> repository.hasSnapshot(commit.getId()))
				.whenResult(findResult -> {
					if (skipSnapshots <= 0) {
						cb.set(Optional.of(findResult.getCommit()));
					} else {
						findSnapshotImpl(findResult.getCommitParents(), skipSnapshots - 1, cb);
					}
				})
				.whenException(cb::setException);
	}

	private Promise<Set<C>> collectRequiredChunks(K checkpointNode) {
		return repository.getHeads()
				.then(heads ->
						reduceEdges(repository, otSystem, heads, checkpointNode,
								DiffsReducer.of(
										new HashSet<>(),
										(Set<C> accumulatedChunks, List<? extends D> diffs) ->
												union(accumulatedChunks, chunksInDiffs(cubeDiffScheme, diffs)),
										CollectionUtils::union))
								.whenComplete(promiseCleanupCollectRequiredChunks.recordStats()))
				.map(accumulators -> accumulators.values().stream().flatMap(Collection::stream).collect(toSet()))
				.whenComplete(transform(Set::size,
						toLogger(logger, thisMethod(), checkpointNode)));
	}

	private Promise<Void> cleanup(K checkpointNode, Set<C> requiredChunks, Instant chunksCleanupTimestamp) {
		return chunksStorage.checkRequiredChunks(requiredChunks)
				.then($ -> repository.cleanup(checkpointNode)
						.whenComplete(promiseCleanupRepository.recordStats()))
				.then($ -> chunksStorage.cleanup(requiredChunks, chunksCleanupTimestamp)
						.whenComplete(promiseCleanupChunks.recordStats()))
				.whenComplete(logger.isLoggable(Level.FINE) ?
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
	public PromiseStats getPromiseCleanup() {
		return promiseCleanup;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupCollectRequiredChunks() {
		return promiseCleanupCollectRequiredChunks;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupRepository() {
		return promiseCleanupRepository;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupChunks() {
		return promiseCleanupChunks;
	}

	@JmxOperation
	public void cleanupNow() {
		cleanup();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private static <T, R> Callback<R> transform(Function<? super R, ? extends T> fn, Callback<? super T> toConsumer) {
		return (value, e) -> toConsumer.accept(value != null ? fn.apply(value) : null, e);
	}
}
