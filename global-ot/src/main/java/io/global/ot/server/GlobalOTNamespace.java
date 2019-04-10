package io.global.ot.server;

import io.datakernel.async.*;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.util.CollectionUtils;
import io.datakernel.util.ref.Ref;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RawCommitHead;
import io.global.ot.api.RepoID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.async.Promises.firstSuccessful;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNamespace extends AbstractGlobalNamespace<GlobalOTNamespace, GlobalOTNodeImpl, GlobalOTNode> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalOTNamespace.class);

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
				.then(masters -> firstSuccessful(masters.stream()
						.map(master -> AsyncSupplier.cast(() ->
								master.list(space))))
						.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptySet())))
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
		private final AsyncSupplier<Void> push = reuse(this::doPush);
		private final AsyncSupplier<Void> pushSnapshots = reuse(this::doPushSnapshots);
		private final AsyncSupplier<Void> pushPullRequests = reuse(this::doPushPullRequests);
		private final Function<Set<SignedData<RawCommitHead>>, Promise<Void>> saveHeads = Promises.coalesce(this::doSaveHeads, CollectionUtils::union);

		private final Map<RawServerId, MasterRepository> masterRepositories = new HashMap<>();

		@Nullable
		private Set<SignedData<RawCommitHead>> polledHeads;

		@Nullable
		private SettablePromise<Void> pollNotifier;

		RepositoryEntry(RepoID repositoryId) {
			this.repositoryId = repositoryId;
		}

		public void start() {
			Promises.loop(AsyncPredicate.of($ -> node.pollMasterRepositories), this::doPollMasterHeads);
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

		public AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads() {
			Ref<Set<SignedData<RawCommitHead>>> lastHeads = new Ref<>(emptySet());
			return () -> Promises.<Set<SignedData<RawCommitHead>>>until(
					() -> node.getCommitStorage().getHeads(repositoryId).map(Map::values).map(HashSet::new),
					polledHeads -> !lastHeads.get().containsAll(polledHeads) ?
							Promise.of(true) :
							ensurePollNotifier().map($ -> false))
					.whenResult(lastHeads::set);
		}

		@NotNull
		private Promise<Void> doPollMasterHeads() {
			return ensureMasterRepositories()
					.then(masterRepositories -> Promises.until(
							() -> Promises.any(masterRepositories.values().stream().map(MasterRepository::poll))
									.map($ -> masterRepositories.values()
											.stream()
											.map(MasterRepository::getHeads)
											.filter(Objects::nonNull)
											.flatMap(Collection::stream)
											.collect(toSet())),
							AsyncPredicate.of(polledHeads -> {
								boolean found = this.polledHeads == null || !this.polledHeads.containsAll(polledHeads);
								this.polledHeads = polledHeads;
								return found;
							})))
					.then(this::saveHeads)
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
											master.listSnapshots(repositoryId, localSnapshotIds)
													.then(newSnapshotIds -> Promises.toList(
															newSnapshotIds.stream()
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
				//noinspection OptionalGetWithoutIsPresent - snapshot presence is checked in node.getCommitStorage().listSnapshotIds()
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
			PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(Collections.reverseOrder());
			return Promises.all(heads.stream()
					.map(head -> node.loadCommit(repositoryId, head)
							.whenResult(commit ->
									queue.add(new RawCommitEntry(head, commit)))))
					.then($ -> Promise.<Set<CommitId>>ofCallback(cb ->
							doExcludeParents(
									queue,
									queue.stream().mapToLong(entry -> entry.commit.getLevel()).min().orElse(0L),
									new HashSet<>(heads),
									cb)))
					.whenComplete(toLogger(logger, "excludeParents", heads, this));
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
							.filter(commitId -> !commitId.isRoot() && queue.stream().map(RawCommitEntry::getCommitId).noneMatch(commitId::equals))
							.map(parentId ->
									node.loadCommit(repositoryId, parentId)
											.whenResult(parentRawCommit ->
													queue.add(new RawCommitEntry(parentId, parentRawCommit)))))
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
