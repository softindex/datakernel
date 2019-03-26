package io.global.ot.server;

import io.datakernel.async.*;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.util.CollectionUtils;
import io.datakernel.util.Tuple2;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.GlobalNamespace;
import io.global.ot.api.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.async.Promises.firstSuccessful;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.Collections.emptySet;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.toSet;

public final class LocalGlobalOTNamespace extends GlobalNamespace<LocalGlobalOTNamespace, LocalGlobalOTNode, GlobalOTNode> {
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalOTNamespace.class);

	private final Map<RepoID, RepositoryEntry> repositories = new HashMap<>();

	private final AsyncSupplier<Void> updateRepositories = reuse(this::doUpdateRepositories);

	private long updateRepositoriesTimestamp;

	public LocalGlobalOTNamespace(LocalGlobalOTNode node, PubKey pubKey) {
		super(node, pubKey);
	}

	public RepositoryEntry ensureRepository(RepoID repositoryId) {
		return repositories.computeIfAbsent(repositoryId, RepositoryEntry::new);
	}

	public Promise<Void> updateRepositories() {
		return updateRepositories.get();
	}

	@NotNull
	private Promise<Void> doUpdateRepositories() {
		logger.trace("Updating repositories");
		if (updateRepositoriesTimestamp > node.now.currentTimeMillis() - node.getLatencyMargin().toMillis()) {
			return Promise.complete();
		}
		return ensureMasterNodes()
				.then(masters -> firstSuccessful(masters.stream()
						.map(master -> AsyncSupplier.cast(() ->
								master.list(space))))
						.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptySet()))).whenResult((Consumer<? super Set<String>>) repoNames -> repoNames.forEach(name -> ensureRepository(RepoID.of(space, name)))).whenResult((Consumer<? super Set<String>>) $ -> updateRepositoriesTimestamp = node.now.currentTimeMillis())
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
		private final AsyncSupplier<Void> push = reuse(this::doPush);
		private final AsyncSupplier<Void> pushSnapshots = reuse(this::doPushSnapshots);
		private final AsyncSupplier<Void> pushPullRequests = reuse(this::doPushPullRequests);
		private final Function<Set<SignedData<RawCommitHead>>, Promise<Void>> saveHeads = Promises.coalesce(this::doSaveHeads, CollectionUtils::union);

		private final LongPolling<Set<SignedData<RawCommitHead>>> longPollingHeads;

		private final Map<RawServerId, MasterRepository> masterRepositories = new HashMap<>();
		private Set<SignedData<RawCommitHead>> polledHeads;

		RepositoryEntry(RepoID repositoryId) {
			this.repositoryId = repositoryId;
			longPollingHeads = new LongPolling<>(
					() -> node.getCommitStorage().getHeads(repositoryId).map(Map::values).map(HashSet::new));
		}

		public LongPolling<Set<SignedData<RawCommitHead>>> getLongPollingHeads() {
			return longPollingHeads;
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
					.then($ -> {
						difference(masterRepositories.keySet(), masterNodes.keySet()).forEach(masterRepositories::remove);
						return Promises.all(difference(masterNodes.keySet(), masterRepositories.keySet())
								.stream()
								.map(serverId -> {
									GlobalOTNode node = masterNodes.get(serverId);
									return Promises.toTuple(Tuple2::new,
											node.getHeads(repositoryId),
											node.getPullRequests(repositoryId)).whenResult((Consumer<? super Tuple2<Set<SignedData<RawCommitHead>>, Set<SignedData<RawPullRequest>>>>) tuple -> masterRepositories.put(serverId,
											new MasterRepository(serverId, repositoryId, node,
													tuple.getValue1(), tuple.getValue2())));
								}));
					})
					.map($ -> masterRepositories);
		}

		@NotNull
		private Promise<Void> doPollHeads() {
			return ensureMasterRepositories()
					.then(masterRepositories1 -> Promises.until(
							() -> Promises.any(masterRepositories1.values().stream().map(MasterRepository::poll))
									.map($ -> masterRepositories1.values().stream()
											.flatMap(masterRepository -> masterRepository.getHeads().stream())
											.collect(toSet())),
							AsyncPredicate.of(polledHeads1 -> {
								boolean found = this.polledHeads == null || !this.polledHeads.containsAll(polledHeads1);
								this.polledHeads = polledHeads1;
								return found;
							})))
					.then(this::saveHeads).whenResult((Consumer<? super Void>) $1 -> updateHeadsTimestamp = node.now.currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdate() {
			if (updateTimestamp > node.now.currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return Promises.all(updateHeads(), updatePullRequests(), updateSnapshots()).whenResult((Consumer<? super Void>) $ -> updateTimestamp = node.now.currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdateHeads() {
			logger.trace("Updating heads");
			if (updateHeadsTimestamp > node.now.currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.then(masters -> node.getCommitStorage().getHeads(repositoryId)
							.then(heads -> firstSuccessful(masters.stream()
									.map(master -> AsyncSupplier.cast(() ->
											master.getHeads(repositoryId)))))
							.mapEx((result, e) -> e == null ? result : Collections.<SignedData<RawCommitHead>>emptySet())
							.then(this::saveHeads)).whenResult((Consumer<? super Void>) $ -> updateHeadsTimestamp = node.now.currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdateSnapshots() {
			logger.trace("Updating snapshots");
			if (updateSnapshotsTimestamp > node.now.currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.then(masters -> node.getCommitStorage().listSnapshotIds(repositoryId)
							.then(localSnapshotIds -> firstSuccessful(masters.stream()
									.map(master -> AsyncSupplier.cast(() ->
											master.listSnapshots(repositoryId, localSnapshotIds)
													.then(newSnapshotIds -> Promises.toList(
															newSnapshotIds.stream()
																	.map(snapshotId -> master.loadSnapshot(repositoryId, snapshotId)
																			.then(Promise::ofOptional)))))))
									.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<SignedData<RawSnapshot>>emptyList())))
							.then(snapshots -> Promises.all(snapshots.stream().map(node.getCommitStorage()::saveSnapshot)))).whenResult((Consumer<? super Void>) $ -> updateSnapshotsTimestamp = node.now.currentTimeMillis());
		}

		@NotNull
		private Promise<Void> doUpdatePullRequests() {
			logger.trace("Updating pull requests");
			if (updatePullRequestsTimestamp > node.now.currentTimeMillis() - node.getLatencyMargin().toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.then(masters -> firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() ->
									master.getPullRequests(repositoryId))))
							.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<SignedData<RawPullRequest>>emptySet())))
					.then(pullRequests -> Promises.all(
							pullRequests.stream().map(node.getCommitStorage()::savePullRequest))).whenResult((Consumer<? super Void>) $ -> updatePullRequestsTimestamp = node.now.currentTimeMillis());
		}

		private Promise<Void> doSaveHeads(Set<SignedData<RawCommitHead>> signedNewHeads) {
			if (signedNewHeads.isEmpty()) {
				return Promise.complete();
			}
			return node.getCommitStorage().getHeads(repositoryId)
					.map(Map::keySet)
					.then(existingHeads -> {
						Set<CommitId> newHeads = signedNewHeads.stream()
								.map(signedNewHead -> signedNewHead.getValue().getCommitId())
								.collect(toSet());
						if (existingHeads.containsAll(newHeads)) {
							return Promise.complete();
						}
						return excludeParents(union(existingHeads, newHeads))
								.then(realHeads -> node.getCommitStorage().updateHeads(repositoryId,
										signedNewHeads.stream().filter(signedNewHead -> realHeads.contains(signedNewHead.getValue().getCommitId())).collect(toSet()),
										difference(existingHeads, realHeads)).whenComplete((Callback<? super Void>) ($, e) -> longPollingHeads.wakeup())
								);
					});
		}

		@NotNull
		private Promise<Void> doFetch() {
			return forEachMaster(master -> {
				logger.trace("{} fetching from {}", repositoryId, master);
				return node.getLocalHeadsInfo(repositoryId)
						.then(headsInfo -> ChannelSupplier.ofPromise(master.download(repositoryId, headsInfo.getRequired(),
								headsInfo.getExisting()))
								.streamTo(ChannelConsumer.ofPromise(node.uploadLocal(repositoryId))));
			});
		}

		@NotNull
		private Promise<Void> doPush() {
			return forEachMaster(master -> {
				logger.trace("{} pushing to {}", repositoryId, master);
				return master.getHeadsInfo(repositoryId)
						.then(headsInfo -> ChannelSupplier.ofPromise(node.download(repositoryId, headsInfo.getRequired(), headsInfo.getExisting()))
								.streamTo(ChannelConsumer.ofPromise(master.upload(repositoryId))));
			});
		}

		@NotNull
		private Promise<Void> doPushSnapshots() {
			return forEachMaster(master -> {
				logger.trace("{} pushing snapshots to {}", repositoryId, master);
				//noinspection OptionalGetWithoutIsPresent - snapshot presence is checked in commitStorage.listSnapshotIds()
				return master.listSnapshots(repositoryId, emptySet())
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
			PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
			return Promises.all(heads.stream()
					.map(head -> {
						return node.loadCommit(repositoryId, head).whenResult((Consumer<? super RawCommit>) commit ->
								queue.add(new RawCommitEntry(head, commit)));
					}))
					.then(value -> Promise.<Set<CommitId>>ofCallback(cb ->
							doExcludeParents(
									queue,
									queue.stream().mapToLong(entry -> entry.commit.getLevel()).min().orElse(0L),
									new HashSet<>(heads),
									cb))).whenComplete(toLogger(logger, "excludeParents", heads, this));
		}

		private void doExcludeParents(PriorityQueue<RawCommitEntry> queue, long minLevel,
									  Set<CommitId> resultHeads, SettableCallback<Set<CommitId>> cb) {
			RawCommitEntry entry = queue.poll();
			if (entry == null || entry.commit.getLevel() < minLevel) {
				cb.set(resultHeads);
				return;
			}
			resultHeads.removeAll(entry.commit.getParents());
			Promises.all(
					entry.commit.getParents()
							.stream()
							.filter(commitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(commitId::equals))
							.map(parentId -> {
								return node.loadCommit(repositoryId, parentId).whenResult((Consumer<? super RawCommit>) parentRawCommit ->
										queue.add(new RawCommitEntry(parentId, parentRawCommit)));
							})).whenResult((Consumer<? super Void>) $ -> doExcludeParents(
					queue, minLevel,
					resultHeads,
					cb)).whenException(cb::setException);
		}

		private Promise<Void> forEachMaster(Function<GlobalOTNode, Promise<Void>> action) {
			return ensureMasterNodes()
					.then(masters -> tolerantCollectVoid(masters, action));
		}
		// endregion
	}
}
