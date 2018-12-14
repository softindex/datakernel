/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.ot.server;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.UncheckedException;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.ot.api.*;
import io.global.ot.server.GlobalOTNodeImpl.PubKeyEntry.RepositoryEntry;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.resubscribe;
import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.common.api.AnnouncementStorage.NO_ANNOUNCEMENT;
import static java.util.Collections.emptySet;
import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNodeImpl implements GlobalOTNode, EventloopService, Initializable<GlobalOTNodeImpl> {
	public static final Duration DEFAULT_LATENCY_MARGIN = Duration.ofMinutes(5);
	private final Eventloop eventloop;
	private final DiscoveryService discoveryService;
	private final CommitStorage commitStorage;
	private final NodeFactory<GlobalOTNode> nodeFactory;
	private final RawServerId id;
	private final Set<PubKey> managedPubKeys = new HashSet<>();

	private final Map<PubKey, PubKeyEntry> pubKeys = new HashMap<>();

	private Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private GlobalOTNodeImpl(Eventloop eventloop, RawServerId id, DiscoveryService discoveryService, @Nullable CommitStorage commitStorage, NodeFactory<GlobalOTNode> nodeFactory) {
		this.eventloop = checkNotNull(eventloop);
		this.id = checkNotNull(id);
		this.discoveryService = checkNotNull(discoveryService);
		this.commitStorage = checkNotNull(commitStorage);
		this.nodeFactory = checkNotNull(nodeFactory);
	}

	public static GlobalOTNodeImpl create(Eventloop eventloop, RawServerId id, DiscoveryService discoveryService, CommitStorage commitStorage, NodeFactory<GlobalOTNode> nodeFactory) {
		return new GlobalOTNodeImpl(eventloop, id, discoveryService, commitStorage, nodeFactory);
	}

	public GlobalOTNodeImpl withLatencyMargin(Duration latencyMargin) {
		this.latencyMargin = latencyMargin;
		return this;
	}

	public GlobalOTNodeImpl withManagedPubKeys(Set<PubKey> managedPubKeys) {
		this.managedPubKeys.addAll(managedPubKeys);
		return this;
	}

	private PubKeyEntry ensurePubKey(PubKey pubKey) {
		return pubKeys.computeIfAbsent(pubKey, PubKeyEntry::new);
	}

	private RepositoryEntry ensureRepository(RepoID repositoryId) {
		return ensurePubKey(repositoryId.getOwner()).ensureRepository(repositoryId);
	}

	private Promise<List<GlobalOTNode>> ensureMasterNodes(PubKey pubKey) {
		return ensurePubKey(pubKey).ensureMasterNodes();
	}

	private Promise<List<GlobalOTNode>> ensureMasterNodes(RepoID repositoryId) {
		return ensureMasterNodes(repositoryId.getOwner());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		PubKeyEntry entry = ensurePubKey(pubKey);
		return entry
				.updateRepositories()
				.thenApply($ -> new HashSet<>(
						entry.repositories.keySet()
								.stream()
								.map(RepoID::getName)
								.collect(toSet())));
	}

	@Override
	public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> newCommits, Set<SignedData<RawCommitHead>> newHeads) {
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Promises.all(
						newCommits.entrySet()
								.stream()
								.map(entry -> commitStorage.saveCommit(entry.getKey(), entry.getValue())))
						.thenCompose($ -> {
							Set<CommitId> excludedHeads = new HashSet<>();
							for (RawCommit commit : newCommits.values()) {
								excludedHeads.addAll(intersection(thisHeads.keySet(), commit.getParents()));
							}
							return updateHeads(repositoryId, new Heads(newHeads, excludedHeads));
						}))
				.thenCompose($ -> isMasterFor(repositoryId) ?
						Promise.complete() :
						ensureMasterNodes(repositoryId)
								.thenCompose(nodes -> Promises.firstSuccessful(nodes.stream()
										.map(node -> node.save(repositoryId, newCommits, newHeads)))));
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return commitStorage.loadCommit(id)
				.thenCompose(maybeCommit -> maybeCommit.isPresent() || isMasterFor(repositoryId) ?
						Promise.ofOptional(maybeCommit) :
						ensureMasterNodes(repositoryId)
								.thenCompose(nodes -> Promises.firstSuccessful(
										nodes.stream()
												.map(node -> node.loadCommit(repositoryId, id))))
								.thenCompose(commit -> commitStorage.saveCommit(id, commit)
										.thenApply($ -> commit))
				);
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> required, Set<CommitId> existing) {
		checkArgument(!hasIntersection(required, existing), "Required heads and existing heads cannot have intersections");
		Set<CommitId> skipCommits = new HashSet<>(existing);
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Promises.all(
						union(thisHeads.keySet(), required, existing)
								.stream()
								.map(commitId -> commitStorage.loadCommit(commitId)
										.whenResult(maybeCommit -> maybeCommit.ifPresent(commit ->
												queue.add(new RawCommitEntry(commitId, commit))))))
						.thenApply($ -> AsyncSupplier.of(() -> getNextStreamEntry(queue, skipCommits, required, existing)))
						.thenApply(ChannelSupplier::of)
						.thenApply(supplier -> supplier.map(
								entry -> new CommitEntry(entry.commitId, entry.commit, thisHeads.get(entry.commitId))))
				);
	}

	private Promise<RawCommitEntry> getNextStreamEntry(PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> required, Set<CommitId> existing) {
		return Promise.ofCallback(cb -> getNextStreamEntry(queue, skipCommits, required, existing, cb));
	}

	private void getNextStreamEntry(PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> required, Set<CommitId> existing,
			SettablePromise<RawCommitEntry> cb) {
		if (queue.isEmpty() || queue.stream().map(RawCommitEntry::getCommitId).allMatch(skipCommits::contains)) {
			cb.set(null);
			return;
		}
		RawCommitEntry entry = queue.poll();
		boolean skipped = skipCommits.remove(entry.commitId);
		Set<CommitId> nextCommitIds = entry.getCommit().getParents();
		Promises.all(
				nextCommitIds
						.stream()
						.filter(nextCommitId -> !existing.contains(nextCommitId))
						.filter(nextCommitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(nextCommitId::equals))
						.map(nextCommitId -> commitStorage.loadCommit(nextCommitId)
								.whenResult(maybeNextCommit -> maybeNextCommit.ifPresent(nextCommit -> {
									if (skipped && !required.contains(nextCommitId)) {
										skipCommits.add(nextCommitId);
									}
									queue.add(new RawCommitEntry(nextCommitId, nextCommit));
								}))))
				.whenResult($ -> {
					if (!skipped) {
						cb.set(entry);
						return;
					}
					getNextStreamEntry(queue, skipCommits, required, existing, cb);
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		HashSet<CommitId> existing = new HashSet<>();
		HashSet<CommitId> required = new HashSet<>();
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return ensureRepository(repositoryId)
				.updateHeads()
				.thenCompose($ -> commitStorage.getHeads(repositoryId))
				.thenApply(Map::keySet)
				.thenCompose(heads -> Promises.all(
						heads.stream()
								.map(headId -> commitStorage.loadCommit(headId)
										.whenResult(maybeCommit -> {
											if (maybeCommit.isPresent()) {
												existing.add(headId);
												queue.add(new RawCommitEntry(headId, maybeCommit.get()));
											} else {
												required.add(headId);
											}
										})))
						.thenCompose($ -> Promise.ofCallback((SettablePromise<Set<CommitId>> cb) ->
								findMissingParents(queue, new HashSet<>(), cb)))
						.whenResult(required::addAll)
						.thenApply($ -> new HeadsInfo(existing, required)));
	}

	private void findMissingParents(PriorityQueue<RawCommitEntry> queue, Set<CommitId> missingParents, SettablePromise<Set<CommitId>> cb) {
		RawCommitEntry entry = queue.poll();
		if (entry == null) {
			cb.set(missingParents);
			return;
		}
		Promises.all(
				entry.commit.getParents()
						.stream()
						.filter(commitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(commitId::equals))
						.map(parentId -> commitStorage.isCompleteCommit(parentId)
								.thenCompose(isCompleteCommit -> isCompleteCommit ?
										Promise.complete() :
										commitStorage.loadCommit(parentId)
												.whenResult(maybeCommit -> {
													if (maybeCommit.isPresent()) {
														queue.add(new RawCommitEntry(parentId, maybeCommit.get()));
													} else {
														missingParents.add(parentId);
													}
												})
												.toVoid())))
				.whenResult($ -> findMissingParents(queue, missingParents, cb))
				.whenException(cb::setException);
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId) {
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenApply(heads -> {
					Set<CommitId> excludedHeads = new HashSet<>();
					Set<SignedData<RawCommitHead>> addedHeads = new HashSet<>();
					return ChannelConsumer.of(
							(CommitEntry entry) -> {
								for (CommitId parentId : entry.getCommit().getParents()) {
									if (heads.contains(parentId)) {
										excludedHeads.add(parentId);
									}
								}
								if (entry.hasHead()) {
									addedHeads.add(entry.getHead());
								}
								return commitStorage.saveCommit(entry.commitId, entry.commit).toVoid();
							})
							.withAcknowledgement(ack -> ack
									.thenCompose($ -> commitStorage.markCompleteCommits())
									.thenCompose($ -> updateHeads(repositoryId, new Heads(addedHeads, excludedHeads))));
				});
	}

	private Promise<Void> updateHeads(RepoID repositoryId, Heads heads) {
		return commitStorage.updateHeads(repositoryId, heads.newHeads, heads.excludedHeads)
				.whenResult($ -> {
					if (!heads.newHeads.isEmpty()) ensureRepository(repositoryId).filterHeads();
				});
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return commitStorage.saveSnapshot(encryptedSnapshot)
				.thenCompose(saved -> saved && !isMasterFor(repositoryId) ?
						ensureMasterNodes(repositoryId)
								.thenCompose(nodes -> Promises.firstSuccessful(nodes.stream()
										.map(node -> node.saveSnapshot(repositoryId, encryptedSnapshot)))) :
						Promise.complete());
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		return commitStorage.loadSnapshot(repositoryId, commitId)
				.thenCompose(maybeSnapshot -> (maybeSnapshot.isPresent() || isMasterFor(repositoryId)) ?
						Promise.of(maybeSnapshot) :
						ensureMasterNodes(repositoryId)
								.thenCompose(nodes -> Promises.firstSuccessful(
										nodes.stream()
												.map(node -> node.loadSnapshot(repositoryId, commitId)
														.thenCompose(Promise::ofOptional)))
										.thenCompose(snapshot -> commitStorage.saveSnapshot(snapshot)
												.thenApply($ -> snapshot)))
								.thenApplyEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.empty()));
	}

	@Override
	public Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
		return ensureRepository(repositoryId)
				.updateHeads()
				.thenCompose($ -> commitStorage.getHeads(repositoryId))
				.thenCompose(heads -> Promise.of(new Heads(
						difference(heads.keySet(), remoteHeads)
								.stream()
								.map(heads::get)
								.collect(toSet()), emptySet()))
				);
	}

	public Promise<Set<CommitId>> excludeParents(Set<CommitId> heads) {
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return Promises.all(heads.stream()
				.map(head -> commitStorage.loadCommit(head)
						.whenResult(optional -> optional.ifPresent(commit ->
								queue.add(new RawCommitEntry(head, commit))))))
				.thenCompose(value -> Promise.<Set<CommitId>>ofCallback(cb ->
						doExcludeParents(
								queue,
								queue.stream().mapToLong(entry -> entry.commit.getLevel()).min().orElse(0L),
								new HashSet<>(heads),
								cb)))
				.thenApply(filteredHeads -> difference(heads, filteredHeads));
	}

	private void doExcludeParents(PriorityQueue<RawCommitEntry> queue, long minLevel,
			Set<CommitId> resultHeads,
			SettablePromise<Set<CommitId>> cb) {
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
						.map(parentId ->
								commitStorage.loadCommit(parentId)
										.whenResult(optional -> optional.ifPresent(
												parentRawCommit ->
														queue.add(new RawCommitEntry(parentId, parentRawCommit))))))
				.whenResult($ -> doExcludeParents(
						queue, minLevel,
						resultHeads,
						cb))
				.whenException(cb::setException);
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return discoveryService.shareKey(receiver, simKey);
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return discoveryService.getSharedKey(receiver, hash);
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return discoveryService.getSharedKeys(receiver);
	}

	@Override
	public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		return commitStorage.savePullRequest(pullRequest)
				.thenCompose(saveStatus -> (saveStatus && !isMasterFor(pullRequest.getValue().getRepository())) ?
						ensureMasterNodes(pullRequest.getValue().repository)
								.thenCompose(nodes -> Promises.any(
										nodes.stream().map(node -> node.sendPullRequest(pullRequest)))) :
						Promise.complete());
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return ensureRepository(repositoryId)
				.updatePullRequests()
				.thenCompose($ -> commitStorage.getPullRequests(repositoryId));
	}

	private boolean isMasterFor(RepoID repositoryId) {
		return managedPubKeys.contains(repositoryId.getOwner());
	}

	class PubKeyEntry {
		private final PubKey pubKey;
		private final Map<RepoID, RepositoryEntry> repositories = new HashMap<>();

		private final Map<RawServerId, GlobalOTNode> masterNodes = new HashMap<>();
		private long updateNodesTimestamp;
		private long updateRepositoriesTimestamp;
		private long announceTimestamp;

		private final AsyncSupplier<List<GlobalOTNode>> ensureMasterNodes = reuse(this::doEnsureMasterNodes);
		private final AsyncSupplier<Void> updateRepositories = reuse(this::doUpdateRepositories);

		PubKeyEntry(PubKey pubKey) {
			this.pubKey = pubKey;
		}

		public RepositoryEntry ensureRepository(RepoID repositoryId) {
			return repositories.computeIfAbsent(repositoryId, RepositoryEntry::new);
		}

		public Promise<List<GlobalOTNode>> ensureMasterNodes() {
			return ensureMasterNodes.get();
		}

		public Promise<Void> updateRepositories() {
			return updateRepositories.get();
		}

		private Promise<List<GlobalOTNode>> doEnsureMasterNodes() {
			if (updateNodesTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.of(getMasterNodes());
			}
			return discoveryService.find(pubKey)
					.thenApplyEx((announceData, e) -> {
						if (e != null) {
							if (e != NO_ANNOUNCEMENT) {
								throw new UncheckedException(e);
							}
						} else if (announceData.verify(pubKey)) {
							AnnounceData announce = announceData.getValue();
							if (announce.getTimestamp() >= announceTimestamp) {
								Set<RawServerId> newServerIds = new HashSet<>(announce.getServerIds());
								masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
								if (newServerIds.remove(id)) {
									managedPubKeys.add(pubKey);
								} else {
									managedPubKeys.remove(pubKey);
								}
								newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, nodeFactory::create));
								updateNodesTimestamp = now.currentTimeMillis();
								announceTimestamp = announce.getTimestamp();
							}
						}
						return getMasterNodes();
					});
		}

		private Promise<Void> doUpdateRepositories() {
			if (updateRepositoriesTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.thenCompose(nodes -> nodes.isEmpty() ?
							Promise.complete() :
							Promises.firstSuccessful(
									nodes.stream().map(node -> node.list(pubKey)))
									.whenResult(repoNames -> repoNames.forEach(name -> ensureRepository(RepoID.of(pubKey, name))))
									.whenResult($ -> updateRepositoriesTimestamp = now.currentTimeMillis())
									.toVoid());
		}

		public Promise<Void> forEach(Function<RepositoryEntry, Promise<Void>> fn) {
			return Promises.all(repositories.values().stream().map(fn));
		}

		public List<GlobalOTNode> getMasterNodes() {
			return new ArrayList<>(masterNodes.values());
		}

		class RepositoryEntry {
			private final RepoID repositoryId;

			private long updateTimestamp;
			private long updateHeadsTimestamp;
			private long updatePullRequestsTimestamp;

			private final AsyncSupplier<Void> update = reuse(this::doUpdate);
			private final AsyncSupplier<Void> updateHeads = reuse(this::doUpdateHeads);
			private final AsyncSupplier<Void> updatePullRequests = reuse(this::doUpdatePullRequests);
			private final AsyncSupplier<Void> fetch = reuse(this::doFetch);
			private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);
			private final AsyncSupplier<Void> filterHeads = resubscribe(this::doFilterHeads);
			private final AsyncSupplier<Void> push = reuse(this::doPush);

			RepositoryEntry(RepoID repositoryId) {
				this.repositoryId = repositoryId;
			}

			public Promise<Void> update() {
				return update.get();
			}

			public Promise<Void> updateHeads() {
				return updateHeads.get();
			}

			public Promise<Void> updatePullRequests() {
				return updatePullRequests.get();
			}

			public Promise<Void> fetch() {
				return fetch.get();
			}

			public Promise<Void> catchUp() {
				return catchUp.get();
			}

			public Promise<Void> filterHeads() {
				return filterHeads.get();
			}

			public Promise<Void> push() {
				return push.get();
			}

			private Promise<Void> doUpdate() {
				if (updateTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return Promises.all(updateHeads(), updatePullRequests())
						.whenResult($ -> updateTimestamp = now.currentTimeMillis());
			}

			private Promise<Void> doUpdateHeads() {
				if (updateHeadsTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return ensureMasterNodes()
						.thenCompose(nodes -> nodes.isEmpty() ?
								Promise.complete() :
								commitStorage.getHeads(repositoryId)
										.thenCompose(heads -> Promises.firstSuccessful(
												nodes.stream().map(node -> node.getHeads(repositoryId, heads.keySet()))))
										.thenCompose(headsDelta ->
												commitStorage.updateHeads(repositoryId, headsDelta.newHeads, headsDelta.excludedHeads)))
						.whenResult($ -> updateHeadsTimestamp = now.currentTimeMillis());
			}

			private Promise<Void> doUpdatePullRequests() {
				if (updatePullRequestsTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return ensureMasterNodes()
						.thenCompose(nodes -> nodes.isEmpty() ?
								Promise.complete() :
								Promises.firstSuccessful(
										nodes.stream().map(node -> node.getPullRequests(repositoryId)))
										.thenCompose(pullRequests -> Promises.all(
												pullRequests.stream().map(commitStorage::savePullRequest))))
						.whenResult($ -> updatePullRequestsTimestamp = now.currentTimeMillis())
						.toVoid();
			}

			private Promise<Void> doFetch() {
				return ensureMasterNodes()
						.thenCompose(nodes -> Promises.firstSuccessful(nodes.stream().map(this::doFetch)));
			}

			private Promise<Void> doFetch(GlobalOTNode node) {
				return getHeadsInfo(repositoryId)
						.thenCompose(headsInfo -> node.downloader(repositoryId, headsInfo.getRequired(), headsInfo.getExisting())
								.streamTo(uploader(repositoryId)));
			}

			private Promise<Void> doCatchUp() {
				return Promise.ofCallback(this::doCatchUp);
			}

			private void doCatchUp(SettablePromise<Void> cb) {
				long timestampBegin = now.currentTimeMillis();
				fetch()
						.thenCompose($ -> commitStorage.markCompleteCommits())
						.whenResult($ -> {
							long timestampEnd = now.currentTimeMillis();
							if (timestampEnd - timestampBegin > latencyMargin.toMillis()) {
								cb.set(null);
							} else {
								doCatchUp(cb);
							}
						})
						.whenException(cb::setException);
			}

			private Promise<Void> doFilterHeads() {
				return commitStorage.getHeads(repositoryId)
						.thenCompose(heads -> excludeParents(heads.keySet())
								.thenCompose(excludedHeadIds -> commitStorage.updateHeads(repositoryId, emptySet(), excludedHeadIds)))
						.toVoid();
			}

			private Promise<Void> doPush() {
				return ensureMasterNodes()
						.thenCompose(nodes -> Promises.all(nodes.stream().map(this::doPush).map(Promise::toTry)));
			}

			private Promise<Void> doPush(GlobalOTNode node) {
				return node.getHeadsInfo(repositoryId)
						.thenCompose(headsInfo -> downloader(repositoryId, headsInfo.getRequired(), headsInfo.getExisting())
								.streamTo(node.uploader(repositoryId)));
			}
		}
	}
}
