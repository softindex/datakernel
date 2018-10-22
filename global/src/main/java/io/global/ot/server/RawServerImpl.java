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

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.*;
import io.global.ot.api.*;
import io.global.ot.server.RawServerImpl.PubKeyEntry.RepositoryEntry;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.resubscribe;
import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;

public final class RawServerImpl implements RawServer, EventloopService {
	private final Eventloop eventloop;
	private final RawDiscoveryService discoveryService;
	private final CommitStorage commitStorage;
	private final RawServerFactory rawServerFactory;

	private Duration latencyMargin;

	private final Map<PubKey, PubKeyEntry> pubKeys = new HashMap<>();
	private final Map<PubKey, Map<PubKey, Map<Hash, SignedData<SharedSimKey>>>> sharedKeysDb = new HashMap<>();

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public RawServerImpl(Eventloop eventloop, RawDiscoveryService discoveryService, CommitStorage commitStorage, RawServerFactory rawServerFactory) {
		this.eventloop = eventloop;
		this.discoveryService = discoveryService;
		this.commitStorage = commitStorage;
		this.rawServerFactory = rawServerFactory;
	}

	private PubKeyEntry ensurePubKey(PubKey pubKey) {
		return pubKeys.computeIfAbsent(pubKey, PubKeyEntry::new);
	}

	private RepositoryEntry ensureRepository(RepoID repositoryId) {
		return ensurePubKey(repositoryId.getOwner()).ensureRepository(repositoryId);
	}

	private Map<PubKey, Map<Hash, SignedData<SharedSimKey>>> ensureSharedKeysDb(PubKey receiver) {
		return sharedKeysDb.computeIfAbsent(receiver, $ -> new HashMap<>());
	}

	private Stage<List<RawServer>> ensureServers(PubKey pubKey) {
		return ensurePubKey(pubKey).ensureServers();
	}

	private Stage<List<RawServer>> ensureServers(RepoID repositoryId) {
		return ensureServers(repositoryId.getOwner());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		return null;
	}

	@Override
	public Stage<Void> stop() {
		return Stage.complete();
	}

	@Override
	public Stage<Set<String>> list(PubKey pubKey) {
		return Stage.of(new HashSet<>(
				ensurePubKey(pubKey)
						.repositories.keySet()
						.stream()
						.map(RepoID::getName)
						.collect(toSet())));
	}

	@Override
	public Stage<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> newCommits, Set<SignedData<RawCommitHead>> newHeads) {
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Stages.all(
						newCommits.entrySet()
								.stream()
								.map(entry -> commitStorage.saveCommit(entry.getKey(), entry.getValue())))
						.thenCompose($ -> {
							Set<CommitId> excludedHeads = new HashSet<>();
							for (RawCommit rawCommit : newCommits.values()) {
								excludedHeads.addAll(intersection(thisHeads.keySet(), rawCommit.getParents()));
							}
							return applyHeads(repositoryId, new Heads(newHeads, excludedHeads));
						}));
	}

	@Override
	public Stage<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return commitStorage.loadCommit(id)
				.thenCompose(Stage::ofOptional);
	}

	@Override
	public Stage<SerialSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> bases, Set<CommitId> heads) {
		checkArgument(!hasIntersection(bases, heads));
		Set<CommitId> skipCommits = new HashSet<>(heads);
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Stages.all(
						union(thisHeads.keySet(), bases, heads)
								.stream()
								.map(commitId -> commitStorage.loadCommit(commitId)
										.whenResult(maybeRawCommit -> maybeRawCommit.ifPresent(rawCommit ->
												queue.add(new RawCommitEntry(commitId, rawCommit))))
								))
						.thenApply($ -> AsyncSupplier.of(() -> getNextStreamEntry(queue, skipCommits, bases, heads)))
						.thenApply(supplier -> supplier.transform(
								entry -> new CommitEntry(entry.commitId, entry.rawCommit, thisHeads.get(entry.commitId))))
						.thenApply(SerialSupplier::of));
	}

	public Stage<SerialSupplier<CommitEntry>> getCommitsSupplier(RepoID repositoryId, Set<CommitId> thatBases, Set<CommitId> thatHeads) {
		checkArgument(!hasIntersection(thatBases, thatHeads));
		Set<CommitId> skipCommits = new HashSet<>(thatHeads);
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Stages.all(
						union(thisHeads.keySet(), thatBases, thatHeads)
								.stream()
								.map(commitId -> commitStorage.loadCommit(commitId)
										.whenResult(maybeRawCommit -> maybeRawCommit.ifPresent(rawCommit ->
												queue.add(new RawCommitEntry(commitId, rawCommit))))
								))
						.thenApply($ -> AsyncSupplier.of(() -> getNextStreamEntry(queue, skipCommits, thatBases, thatHeads)))
						.thenApply(supplier -> supplier.transform(
								entry -> new CommitEntry(entry.commitId, entry.rawCommit, thisHeads.get(entry.commitId))))
						.thenApply(SerialSupplier::of));
	}

	private Stage<RawCommitEntry> getNextStreamEntry(PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> thatBases, Set<CommitId> thatHeads) {
		return Stage.ofCallback(cb -> getNextStreamEntry(queue, skipCommits, thatBases, thatHeads, cb));
	}

	private void getNextStreamEntry(PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> thatBases, Set<CommitId> thatHeads,
			SettableStage<RawCommitEntry> cb) {
		if (queue.isEmpty() || queue.stream().map(RawCommitEntry::getCommitId).allMatch(skipCommits::contains)) {
			cb.set(null);
			return;
		}
		RawCommitEntry entry = queue.poll();
		boolean skipped = skipCommits.remove(entry.commitId);
		Set<CommitId> nextCommitIds = entry.getRawCommit().getParents();
		Stages.all(
				nextCommitIds
						.stream()
						.filter(nextCommitId -> !thatHeads.contains(nextCommitId))
						.filter(nextCommitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(nextCommitId::equals))
						.map(nextCommitId -> commitStorage.loadCommit(nextCommitId)
								.whenResult(maybeNextRawCommit -> maybeNextRawCommit.ifPresent(nextRawCommit -> {
									if (skipped && !thatBases.contains(nextCommitId)) {
										skipCommits.add(nextCommitId);
									}
									queue.add(new RawCommitEntry(nextCommitId, nextRawCommit));
								}))))
				.whenResult($ -> {
					if (!skipped) {
						cb.set(entry);
						return;
					}
					getNextStreamEntry(queue, skipCommits, thatBases, thatHeads, cb);
				})
				.whenException(cb::setException);
	}

	@Override
	public Stage<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		HeadsInfo headsInfo = new HeadsInfo(new HashSet<>(), new HashSet<>());
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenCompose(thisHeads -> Stages.all(
						thisHeads
								.stream()
								.map(head -> commitStorage.loadCommit(head)
										.whenResult(maybeRawCommit -> maybeRawCommit.ifPresent(
												rawCommit -> {
													headsInfo.heads.add(head);
													queue.add(new RawCommitEntry(head, rawCommit));
												}
										))))
						.<Set<CommitId>>thenCallback(($, cb) ->
								extractHeadInfoImpl(queue, new HashSet<CommitId>(), cb))
						.whenResult(headsInfo.bases::addAll)
						.thenApply($ -> headsInfo));
	}

	private void extractHeadInfoImpl(PriorityQueue<RawCommitEntry> queue, Set<CommitId> bases, SettableStage<Set<CommitId>> cb) {
		RawCommitEntry entry = queue.poll();
		if (entry == null) {
			cb.set(bases);
			return;
		}
		Stages.all(
				entry.rawCommit.getParents()
						.stream()
						.filter(commitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(commitId::equals))
						.map(parentId -> commitStorage.isCompleteCommit(parentId)
								.thenCompose(isCompleteCommit -> isCompleteCommit ?
										Stage.of(null) :
										commitStorage.loadCommit(parentId)
												.whenResult(optional -> {
													if (optional.isPresent()) {
														queue.add(new RawCommitEntry(parentId, optional.get()));
													} else {
														bases.add(parentId);
													}
												}))))
				.whenResult($ -> extractHeadInfoImpl(queue, bases, cb))
				.whenException(cb::setException);
	}

	@Override
	public Stage<SerialConsumer<CommitEntry>> upload(RepoID repositoryId) {
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenApply(thisHeads -> {
					Set<CommitId> excludedHeads = new HashSet<>();
					Set<SignedData<RawCommitHead>> addedHeads = new HashSet<>();
					return SerialConsumer.of(
							(CommitEntry entry) -> {
								for (CommitId parentId : entry.getCommit().getParents()) {
									if (thisHeads.contains(parentId)) {
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
									.thenCompose($ -> applyHeads(repositoryId, new Heads(addedHeads, excludedHeads))));
				});
	}

	public Stage<SerialConsumer<CommitEntry>> getStreamConsumer(RepoID repositoryId) {
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenApply(thisHeads -> {
					Set<CommitId> excludedHeads = new HashSet<>();
					Set<SignedData<RawCommitHead>> addedHeads = new HashSet<>();
					return SerialConsumer.of(
							(CommitEntry entry) -> {
								for (CommitId parentId : entry.getCommit().getParents()) {
									if (thisHeads.contains(parentId)) {
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
									.thenCompose($ -> applyHeads(repositoryId, new Heads(addedHeads, excludedHeads))));
				});
	}

	private Stage<Void> applyHeads(RepoID repositoryId, Heads heads) {
		return commitStorage.applyHeads(repositoryId, heads.newHeads, heads.excludedHeads)
				.whenResult($ -> {
					if (!heads.newHeads.isEmpty()) ensureRepository(repositoryId).filterHeads();
				});
	}

	@Override
	public Stage<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return commitStorage.saveSnapshot(encryptedSnapshot)
				.thenCompose(saved -> saved ?
						ensureServers(repositoryId)
								.thenCompose(servers -> Stages.any(servers.stream()
										.map(server -> server.saveSnapshot(repositoryId, encryptedSnapshot)))) :
						Stage.complete());
	}

	@Override
	public Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		return Stages.firstSuccessful(
				() -> commitStorage.loadSnapshot(repositoryId, commitId)
						.thenCompose(Stage::ofOptional),
				() -> ensureServers(repositoryId)
						.thenCompose(servers -> Stages.firstSuccessful(
								servers.stream()
										.map(server -> server.loadSnapshot(repositoryId, commitId)
												.thenCompose(Stage::ofOptional)))))
				.thenApplyEx((maybeResult, e) -> e == null ? Optional.of(maybeResult) : Optional.empty());
	}

	@Override
	public Stage<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
		return commitStorage.getHeads(repositoryId)
				.thenCompose(heads -> excludeParents(union(heads.keySet(), remoteHeads))
						.thenApply(excludedHeads -> new Heads(
								difference(heads.keySet(), union(remoteHeads, excludedHeads)).stream()
										.map(heads::get)
										.collect(toSet()),
								excludedHeads)));
	}

	public Stage<Set<CommitId>> excludeParents(Set<CommitId> heads) {
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return Stages.all(heads.stream()
				.map(head -> commitStorage.loadCommit(head)
						.whenResult(optional -> optional.ifPresent(rawCommit ->
								queue.add(new RawCommitEntry(head, rawCommit))))))
				.<Set<CommitId>>thenCallback(($, cb) ->
						doExcludeParents(
								queue,
								queue.stream().mapToLong(entry -> entry.rawCommit.getLevel()).min().orElse(0L),
								new HashSet<>(heads),
								cb))
				.thenApply(filteredHeads -> difference(heads, filteredHeads));
	}

	private void doExcludeParents(PriorityQueue<RawCommitEntry> queue, long minLevel,
			Set<CommitId> resultHeads,
			SettableStage<Set<CommitId>> cb) {
		RawCommitEntry entry = queue.poll();
		if (entry == null || entry.rawCommit.getLevel() < minLevel) {
			cb.set(resultHeads);
			return;
		}
		resultHeads.removeAll(entry.rawCommit.getParents());
		Stages.all(
				entry.rawCommit.getParents()
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
	public Stage<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey) {
		ensureSharedKeysDb(simKey.getData().getReceiver())
				.computeIfAbsent(owner, $ -> new HashMap<>())
				.put(simKey.getData().getHash(), simKey);
		return Stage.complete();
	}

	@Override
	public Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, Hash simKeyHash) {
		return Stage.of(Optional.ofNullable(
				ensureSharedKeysDb(receiver)
						.getOrDefault(owner, emptyMap())
						.get(simKeyHash)));
	}

	@Override
	public Stage<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		return commitStorage.savePullRequest(pullRequest)
				.thenCompose(saveStatus -> saveStatus ?
						ensureServers(pullRequest.getData().repository)
								.thenCompose(servers -> Stages.any(
										servers.stream().map(server -> server.sendPullRequest(pullRequest))
								)) :
						Stage.complete());
	}

	@Override
	public Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return ensureRepository(repositoryId)
				.update()
				.thenCompose($ -> commitStorage.getPullRequests(repositoryId));
	}

	class PubKeyEntry {
		private final PubKey pubKey;
		private final Map<RepoID, RepositoryEntry> repositories = new HashMap<>();

		private final Map<RawServerId, RawServer> servers = new HashMap<>();
		public long updateServersTimestamp;

		private final AsyncSupplier<List<RawServer>> ensureServers = reuse(this::doEnsureServers);

		PubKeyEntry(PubKey pubKey) {
			this.pubKey = pubKey;
		}

		public RepositoryEntry ensureRepository(RepoID repositoryId) {
			return repositories.computeIfAbsent(repositoryId, RepositoryEntry::new);
		}

		public Stage<List<RawServer>> ensureServers() {
			return ensureServers.get();
		}

		private Stage<List<RawServer>> doEnsureServers() {
			if (updateServersTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Stage.of(getServers());
			}
			return discoveryService.findServers(pubKey)
					.whenResult(announceData -> {
						Set<RawServerId> newServerIds = announceData.getData().getServerIds();
						difference(servers.keySet(), newServerIds).forEach(servers::remove);
						for (RawServerId newServerId : newServerIds) {
							if (servers.containsKey(newServerId)) continue;
							servers.put(newServerId,
									rawServerFactory.create(newServerId));
						}
					})
					.whenResult($ -> updateServersTimestamp = currentTimeMillis())
					.thenApply($ -> getServers());
		}

		public Stage<Void> forEach(Function<RepositoryEntry, Stage<Void>> fn) {
			return Stages.all(repositories.values().stream().map(fn));
		}

		public List<RawServer> getServers() {
			return new ArrayList<>(servers.values());
		}

		class RepositoryEntry {
			private final RepoID repositoryId;

			private long updateTimestamp;

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

			public Stage<Void> update() {
				return update.get();
			}

			public Stage<Void> updateHeads() {
				return updateHeads.get();
			}

			public Stage<Void> updatePullRequests() {
				return updatePullRequests.get();
			}

			public Stage<Void> fetch() {
				return fetch.get();
			}

			public Stage<Void> catchUp() {
				return catchUp.get();
			}

			public Stage<Void> filterHeads() {
				return filterHeads.get();
			}

			public Stage<Void> push() {
				return push.get();
			}

			private Stage<Void> doUpdate() {
				if (updateTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Stage.complete();
				}
				return Stages.all(updateHeads(), updatePullRequests())
						.whenResult($ -> this.updateTimestamp = now.currentTimeMillis());
			}

			private Stage<Void> doUpdateHeads() {
				return ensureServers()
						.thenCompose(servers -> commitStorage.getHeads(repositoryId)
								.thenCompose(heads -> Stages.firstSuccessful(
										servers.stream().map(server -> server.getHeads(repositoryId, heads.keySet()))))
								.thenCompose(headsDelta ->
										commitStorage.applyHeads(repositoryId, headsDelta.newHeads, headsDelta.excludedHeads)));
			}

			private Stage<Void> doUpdatePullRequests() {
				return ensureServers()
						.thenCompose(servers -> Stages.firstSuccessful(
								servers.stream().map(server -> server.getPullRequests(repositoryId))))
						.thenCompose(pullRequests -> Stages.all(
								pullRequests.stream().map(commitStorage::savePullRequest)))
						.toVoid();
			}

			private Stage<Void> doFetch() {
				return ensureServers()
						.thenCompose(servers -> Stages.firstSuccessful(servers.stream().map(this::doFetch)));
			}

			private Stage<Void> doFetch(RawServer server) {
				return getHeadsInfo(repositoryId)
						.thenCompose(headsInfo -> server.downloader(repositoryId, headsInfo.bases, headsInfo.heads)
								.streamTo(SerialConsumer.ofStage(getStreamConsumer(repositoryId))));
			}

			private Stage<Void> doCatchUp() {
				return Stage.ofCallback(this::doCatchUp);
			}

			private void doCatchUp(SettableStage<Void> cb) {
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

			private Stage<Void> doFilterHeads() {
				return commitStorage.getHeads(repositoryId)
						.thenCompose(heads -> excludeParents(heads.keySet())
								.thenCompose(excludedHeadIds -> commitStorage.applyHeads(repositoryId, emptySet(), excludedHeadIds)))
						.toVoid();
			}

			private Stage<Void> doPush() {
				return ensureServers()
						.thenCompose(servers -> Stages.all(servers.stream().map(this::doPush).map(Stage::toTry)));
			}

			private Stage<Void> doPush(RawServer server) {
				return server.getHeadsInfo(repositoryId)
						.thenCompose(headsInfo -> SerialSupplier.ofStage(
								getCommitsSupplier(repositoryId, headsInfo.bases, headsInfo.heads))
								.streamTo(server.uploader(repositoryId)));
			}

		}
	}
}
