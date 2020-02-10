package io.global.ot.server;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.ref.Ref;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.ot.api.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.function.AsyncSuppliers.coalesce;
import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.process.AsyncExecutors.retry;
import static io.datakernel.async.process.Cancellable.CANCEL_EXCEPTION;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.common.collection.CollectionUtils.difference;
import static io.datakernel.common.collection.CollectionUtils.union;
import static io.datakernel.promise.Promises.firstSuccessful;
import static io.datakernel.promise.Promises.toList;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNamespace extends AbstractGlobalNamespace<GlobalOTNamespace, GlobalOTNodeImpl, GlobalOTNode> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalOTNamespace.class);

	public static final StacklessException POLLING_HAS_BEEN_STOPPED = new StacklessException(GlobalOTNodeImpl.class, "Polling has been stopped");

	private final Map<RepoID, RepositoryEntry> repositories = new HashMap<>();

	private final AsyncSupplier<Void> updateRepositories = reuse(this::doUpdateRepositories);

	private long updateRepositoriesTimestamp;

	public GlobalOTNamespace(GlobalOTNodeImpl node, PubKey pubKey) {
		super(node, pubKey);
	}

	public RepositoryEntry ensureRepository(RepoID repositoryId) {
		return repositories.computeIfAbsent(repositoryId, $ -> {
			RepositoryEntry repositoryEntry = new RepositoryEntry(repositoryId);
			repositoryEntry.start();
			return repositoryEntry;
		});
	}

	public Promise<Void> updateRepositories() {
		return updateRepositories.get();
	}

	@NotNull
	private Promise<Void> doUpdateRepositories() {
		logger.trace("Updating repositories");
		if (updateRepositoriesTimestamp > node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
			return Promise.complete();
		}
		return ensureMasterNodes()
				.then(masters -> toList(masters.stream()
						.map(master -> master.list(space)
								.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptySet())))))
				.map(lists -> lists.stream().flatMap(Collection::stream).collect(toSet()))
				.whenResult(repoNames -> repoNames.forEach(name -> ensureRepository(RepoID.of(space, name))))
				.whenResult($ -> updateRepositoriesTimestamp = node.getCurrentTimeProvider().currentTimeMillis())
				.toVoid();
	}

	public Map<RepoID, RepositoryEntry> getRepositories() {
		return repositories;
	}

	public Promise<Void> forEach(Function<RepositoryEntry, Promise<Void>> fn) {
		return Promises.all(repositories.values().stream().map(fn));
	}

	// VisibleForTesting
	Map<RawServerId, GlobalOTNode> getMasters() {
		return masterNodes;
	}

	class RepositoryEntry {
		private final RepoID repositoryId;

		private long updateTimestamp;
		private long updateHeadsTimestamp;
		private long updateSnapshotsTimestamp;
		private long updatePullRequestsTimestamp;

		private final AsyncSupplier<Map<RawServerId, MasterRepository>> ensureMasterRepositories = reuse(this::doEnsureMasterRepositories);
		private final AsyncSupplier<Void> update = reuse(this::doUpdate);
		private final AsyncSupplier<Void> updateHeads = reuse(this::doUpdateHeads);
		private final AsyncSupplier<Void> updateSnapshots = reuse(this::doUpdateSnapshots);
		private final AsyncSupplier<Void> updatePullRequests = reuse(this::doUpdatePullRequests);
		private final AsyncSupplier<Void> fetch = reuse(this::doFetch);
		private final AsyncSupplier<Void> push = coalesce(AsyncSupplier.cast(this::doPush).withExecutor(retry(node.retryPolicy)));
		private final AsyncSupplier<Void> pushSnapshots = reuse(this::doPushSnapshots);
		private final AsyncSupplier<Void> pushPullRequests = reuse(this::doPushPullRequests);
		private final Function<Set<SignedData<RawCommitHead>>, Promise<Void>> saveHeads = Promises.coalesce(HashSet::new, AbstractCollection::addAll, this::doSaveHeads);

		private final Map<RawServerId, MasterRepository> masterRepositories = new HashMap<>();

		@Nullable
		private Set<SignedData<RawCommitHead>> polledHeads;

		@Nullable
		private SettablePromise<Void> pollNotifier;

		RepositoryEntry(RepoID repositoryId) {
			this.repositoryId = repositoryId;
		}

		public void start() {
			Promises.retry(() -> Promises.loop((Void) null, $ -> node.pollMasterRepositories, $ -> doPollMasterHeads()), node.retryPolicy);
		}

		// region API methods
		public Promise<Map<RawServerId, MasterRepository>> ensureMasterRepositories() {
			return ensureMasterRepositories.get();
		}

		public Promise<Void> update() {
			return update.get();
		}

		public Promise<Void> updateHeads() {
			return updateHeads.get();
		}

		public Promise<Void> updateSnapshots() {
			return updateSnapshots.get();
		}

		public Promise<Void> updatePullRequests() {
			return updatePullRequests.get();
		}

		public Promise<Void> fetch() {
			return fetch.get();
		}

		public Promise<Void> push() {
			return push.get();
		}

		public Promise<Void> pushSnapshots() {
			return pushSnapshots.get();
		}

		public Promise<Void> pushPullRequests() {
			return pushPullRequests.get();
		}

		public Promise<Void> saveHeads(Set<SignedData<RawCommitHead>> newHeads) {
			return saveHeads.apply(newHeads);
		}
		// endregion

		// region API methods implementation
		@NotNull
		private Promise<Map<RawServerId, MasterRepository>> doEnsureMasterRepositories() {
			return ensureMasterNodes()
					.whenResult($ -> {
						difference(masterRepositories.keySet(), masterNodes.keySet())
								.forEach(masterRepositories::remove);
						difference(masterNodes.keySet(), masterRepositories.keySet())
								.forEach(serverId -> masterRepositories.put(serverId,
										new MasterRepository(serverId, repositoryId, masterNodes.get(serverId))));
					})
					.map($ -> masterRepositories);
		}

		private Promise<Void> ensurePollNotifier() {
			if (pollNotifier == null) {
				pollNotifier = new SettablePromise<>();
			}
			return pollNotifier;
		}

		private void notifyPoll() {
			if (pollNotifier != null) {
				SettablePromise<@Nullable Void> pollNotifier = this.pollNotifier;
				this.pollNotifier = null;
				pollNotifier.set(null);
			}
		}

		void stopPolling() {
			if (pollNotifier != null) {
				SettablePromise<@Nullable Void> pollNotifier = this.pollNotifier;
				this.pollNotifier = null;
				pollNotifier.setException(POLLING_HAS_BEEN_STOPPED);
			}
		}

		public AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads() {
			Ref<Set<SignedData<RawCommitHead>>> lastHeads = new Ref<>(emptySet());
			return () -> Promises.until(
					false,
					$ -> node.getCommitStorage().getHeads(repositoryId)
							.then(map -> {
								Set<SignedData<RawCommitHead>> heads = new HashSet<>(map.values());
								if (!lastHeads.get().containsAll(heads)) {
									lastHeads.set(heads);
									return Promise.of(true);
								}
								return ensurePollNotifier()
										.map($2 -> false);
							})
					,
					done -> done)
					.map($ -> lastHeads.value);
		}

		@NotNull
		private Promise<Void> doPollMasterHeads() {
			return ensureMasterRepositories()
					.then(masterRepositories -> Promises.<Tuple2<GlobalOTNode, Set<SignedData<RawCommitHead>>>>until(
							null,
							$ -> Promises.any(masterRepositories.values().stream().map(masterRepo -> masterRepo.poll()
									.map($2 -> masterRepo)))
									.map(masterRepo -> new Tuple2<>(masterRepo.getNode(), masterRepositories.values()
											.stream()
											.filter(masterRepository -> masterRepository.getHeads() != null)
											.map(MasterRepository::getHeads)
											.flatMap(Collection::stream)
											.collect(toSet()))),
							nodeAndHeads -> {
								Set<SignedData<RawCommitHead>> polledHeads = nodeAndHeads.getValue2();
								boolean found = this.polledHeads == null || !this.polledHeads.containsAll(polledHeads);
								this.polledHeads = polledHeads;
								return found;
							}))
					.then(nodeAndHeads -> node.isMasterFor(space) ?
							doFetch(nodeAndHeads.getValue1()) :
							saveHeads(nodeAndHeads.getValue2()))
					.whenResult($ -> updateHeadsTimestamp = node.getCurrentTimeProvider().currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdate() {
			if (updateTimestamp > node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			Stream<AsyncSupplier> updates = Stream.of(this::updateHeads, this::updatePullRequests, this::updateSnapshots);
			return ensureMasterNodes()
					.then(masters -> masters.isEmpty() && node.isMasterFor(space) ?
							Promise.complete() :
							tolerantCollectVoid(updates, AsyncSupplier::get))
					.whenResult($ -> updateTimestamp = node.getCurrentTimeProvider().currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdateHeads() {
			logger.trace("Updating heads");
			if (updateHeadsTimestamp > node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.then(masters -> firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() ->
									master.getHeads(repositoryId)))))
					.then(this::saveHeads)
					.whenResult($ -> updateHeadsTimestamp = node.getCurrentTimeProvider().currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdateSnapshots() {
			logger.trace("Updating snapshots");
			if (updateSnapshotsTimestamp >= node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.then(masters -> node.getCommitStorage().listSnapshotIds(repositoryId)
							.then(localSnapshotIds -> firstSuccessful(masters.stream()
									.map(master -> AsyncSupplier.cast(() ->
											master.listSnapshots(repositoryId)
													.then(newSnapshotIds -> Promises.toList(
															difference(localSnapshotIds, newSnapshotIds).stream()
																	.map(snapshotId -> master.loadSnapshot(repositoryId, snapshotId)
																			.then(Promise::ofOptional))))))))
							.then(snapshots -> Promises.all(snapshots.stream().map(node.getCommitStorage()::saveSnapshot))))
					.whenResult($ -> updateSnapshotsTimestamp = node.getCurrentTimeProvider().currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdatePullRequests() {
			logger.trace("Updating pull requests");
			if (updatePullRequestsTimestamp >= node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.then(masters -> firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() ->
									master.getPullRequests(repositoryId)))))
					.then(pullRequests -> Promises.all(
							pullRequests.stream().map(node.getCommitStorage()::savePullRequest)))
					.whenResult($ -> updatePullRequestsTimestamp = node.getCurrentTimeProvider().currentTimeMillis());
		}

		private Promise<Void> doSaveHeads(Set<SignedData<RawCommitHead>> signedNewHeads) {
			if (signedNewHeads.isEmpty()) {
				return Promise.complete();
			}
			return node.getCommitStorage().getHeads(repositoryId)
					.map(Map::keySet)
					.then(existingHeads -> {
						Set<CommitId> newHeads = signedNewHeads.stream().map(signedNewHead -> signedNewHead.getValue().getCommitId()).collect(toSet());
						if (existingHeads.containsAll(newHeads)) {
							return Promise.complete();
						}
						return excludeParents(union(existingHeads, newHeads))
								.then(realHeads -> node.getCommitStorage().updateHeads(repositoryId,
										signedNewHeads.stream().filter(signedNewHead -> realHeads.contains(signedNewHead.getValue().getCommitId())).collect(toSet()),
										difference(existingHeads, realHeads))
										.whenComplete(($, e) -> notifyPoll())
								);
					});
		}

		@NotNull
		private Promise<Void> doFetch() {
			return forEachMaster(this::doFetch);
		}

		@NotNull
		private Promise<Void> doFetch(GlobalOTNode targetNode) {
			logger.trace("{} fetching from {}", repositoryId, targetNode);
			return transfer(targetNode, node);
		}

		@NotNull
		private Promise<Void> doPush() {
			return forEachMaster(this::doPush);
		}

		private Promise<Void> doPush(GlobalOTNode targetNode) {
			logger.trace("{} pushing to {}", repositoryId, targetNode);
			return transfer(node, targetNode);
		}

		private Promise<Void> transfer(GlobalOTNode from, GlobalOTNode to) {
			return from.getHeads(repositoryId)
					.then(heads -> ChannelSupplier.ofPromise(
							from.download(
									repositoryId,
									heads.stream()
											.map(signedHead -> signedHead.getValue().getCommitId())
											.collect(toSet())
							))
							.streamTo(to.upload(repositoryId, new HashSet<>(heads))))
					.thenEx(($, e) -> e == CANCEL_EXCEPTION ? Promise.complete() : Promise.of($, e));
		}

		@NotNull
		private Promise<Void> doPushSnapshots() {
			return forEachMaster(master -> {
				logger.trace("{} pushing snapshots to {}", repositoryId, master);
				//noinspection OptionalGetWithoutIsPresent - snapshot presence is checked in node.getCommitStorage().listSnapshotIds()
				return master.listSnapshots(repositoryId)
						.then(remoteSnapshotIds -> node.getCommitStorage().listSnapshotIds(repositoryId)
								.map(localSnapshotIds -> difference(localSnapshotIds, remoteSnapshotIds)))
						.then(snapshotsIds -> Promises.toList(snapshotsIds.stream()
								.map(snapshot -> node.getCommitStorage().loadSnapshot(repositoryId, snapshot))))
						.then(snapshots -> tolerantCollectVoid(snapshots,
								snapshot -> master.saveSnapshot(repositoryId, snapshot.get())));
			});
		}

		@NotNull
		private Promise<Void> doPushPullRequests() {
			return forEachMaster(master -> {
				logger.trace("{} pushing pull requests to {}", repositoryId, master);
				return master.getPullRequests(repositoryId)
						.then(remotePullRequests -> node.getCommitStorage().getPullRequests(repositoryId)
								.map(localPullRequests -> difference(localPullRequests, remotePullRequests)))
						.then(pullRequests -> tolerantCollectVoid(pullRequests, master::sendPullRequest));
			});
		}
		// endregion

		// region Helper methods
		public Promise<Set<CommitId>> excludeParents(Set<CommitId> heads) {
			PriorityQueue<CommitEntry> queue = new PriorityQueue<>();
			return Promises.all(heads.stream()
					.map(head -> node.loadCommit(repositoryId, head)
							.whenResult(commit ->
									queue.add(new CommitEntry(head, commit)))))
					.then($ -> Promise.<Set<CommitId>>ofCallback(cb ->
							doExcludeParents(
									queue,
									queue.stream().mapToLong(CommitEntry::getLevel).min().orElse(0L),
									new HashSet<>(heads),
									cb)))
					.whenComplete(toLogger(logger, "excludeParents", heads, this));
		}

		private void doExcludeParents(PriorityQueue<CommitEntry> queue, long minLevel,
				Set<CommitId> resultHeads, SettablePromise<Set<CommitId>> cb) {
			CommitEntry entry = queue.poll();
			if (entry == null || entry.getLevel() < minLevel) {
				cb.set(resultHeads);
				return;
			}
			resultHeads.removeAll(entry.getParents());
			Promises.all(
					entry.getParents()
							.stream()
							.filter(commitId -> !commitId.isRoot() && queue.stream().map(CommitEntry::getCommitId).noneMatch(commitId::equals))
							.map(parentId ->
									node.loadCommit(repositoryId, parentId)
											.whenResult(parentRawCommit ->
													queue.add(new CommitEntry(parentId, parentRawCommit)))))
					.whenResult($ -> doExcludeParents(
							queue, minLevel,
							resultHeads,
							cb))
					.whenException(cb::setException);
		}

		private Promise<Void> forEachMaster(Function<GlobalOTNode, Promise<Void>> action) {
			return ensureMasterNodes()
					.then(masters -> tolerantCollectVoid(masters, action));
		}
		// endregion
	}
}
