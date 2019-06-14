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

import io.datakernel.async.*;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.ot.api.*;
import io.global.ot.server.GlobalOTNamespace.RepositoryEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.async.Promises.firstSuccessful;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.LogUtils.Level.FINEST;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.util.Utils.nSuccessesOrLess;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNodeImpl extends AbstractGlobalNode<GlobalOTNodeImpl, GlobalOTNamespace, GlobalOTNode> implements GlobalOTNode, EventloopService, Initializable<GlobalOTNodeImpl> {
	private static final Logger logger = Logger.getLogger(GlobalOTNodeImpl.class.getName());

	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry();
	public static final Boolean DEFAULT_POLL_MASTER_REPOSITORIES = ApplicationSettings.getBoolean(GlobalOTNodeImpl.class, "pollMasterRepositories", true);

	private final Eventloop eventloop;
	private final CommitStorage commitStorage;

	boolean pollMasterRepositories = DEFAULT_POLL_MASTER_REPOSITORIES;
	RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

	private int propagations = 1;

	private GlobalOTNodeImpl(Eventloop eventloop, RawServerId id,
			DiscoveryService discoveryService,
			@Nullable CommitStorage commitStorage,
			Function<RawServerId, GlobalOTNode> nodeFactory) {
		super(id, discoveryService, nodeFactory);
		this.eventloop = checkNotNull(eventloop);
		this.commitStorage = checkNotNull(commitStorage);
	}

	public static GlobalOTNodeImpl create(Eventloop eventloop, RawServerId id,
			DiscoveryService discoveryService, CommitStorage commitStorage,
			Function<RawServerId, GlobalOTNode> nodeFactory) {
		return new GlobalOTNodeImpl(eventloop, id, discoveryService, commitStorage, nodeFactory);
	}

	public GlobalOTNodeImpl withPollMasterRepositories(boolean pollMasterRepositories) {
		this.pollMasterRepositories = pollMasterRepositories;
		return this;
	}

	public GlobalOTNodeImpl withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	@Override
	protected GlobalOTNamespace createNamespace(PubKey space) {
		return new GlobalOTNamespace(this, space);
	}

	@Override
	public RawServerId getId() {
		return id;
	}

	public CommitStorage getCommitStorage() {
		return commitStorage;
	}

	// Visible for testing
	RepositoryEntry ensureRepository(RepoID repositoryId) {
		return ensureNamespace(repositoryId.getOwner()).ensureRepository(repositoryId);
	}

	private Promise<List<GlobalOTNode>> ensureMasterNodes(PubKey pubKey) {
		return ensureNamespace(pubKey).ensureMasterNodes();
	}

	private Promise<List<GlobalOTNode>> ensureMasterNodes(RepoID repositoryId) {
		return ensureMasterNodes(repositoryId.getOwner());
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		if (pollMasterRepositories) {
			pollMasterRepositories = false;
			namespaces.values()
					.stream()
					.flatMap(ns -> ns.getRepositories().values().stream())
					.forEach(RepositoryEntry::stopPolling);
		}
		return Promise.complete();
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		GlobalOTNamespace ns = ensureNamespace(pubKey);
		return ns.ensureMasterNodes()
				.then($ -> isMasterFor(pubKey) ?
						Promise.complete() :
						ns.updateRepositories())
				.map($ -> ns.getRepositories()
						.keySet()
						.stream()
						.map(RepoID::getName)
						.collect(toSet()))
				.whenComplete(toLogger(logger, "list", pubKey, this));
	}

	@Override
	public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> newCommits) {
		return Promises.all(newCommits.entrySet()
				.stream()
				.map(entry -> commitStorage.saveCommit(entry.getKey(), entry.getValue())))
				.whenComplete(toLogger(logger, "save", repositoryId, newCommits, this));
	}

	@Override
	public Promise<Void> saveHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads) {
		if (newHeads.isEmpty()) {
			return Promise.complete();
		}
		RepositoryEntry repositoryEntry = ensureRepository(repositoryId);
		return repositoryEntry.saveHeads(newHeads)
				.whenResult($ -> repositoryEntry.push());
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return tryGetCommit(repositoryId, id)
				.then(Promise::ofOptional)
				.whenComplete(toLogger(logger, "loadCommit", repositoryId, id, this));
	}

	private Promise<Optional<RawCommit>> tryGetCommit(RepoID repositoryId, CommitId commitId) {
		return commitStorage.loadCommit(commitId)
				.then(maybeCommit -> maybeCommit.isPresent() ?
						Promise.of(maybeCommit) :
						fromMaster(repositoryId, node -> node.loadCommit(repositoryId, commitId)
										.then(rawCommit -> commitStorage.saveCommit(commitId, rawCommit)
												.map($ -> Optional.of(rawCommit))),
								Promise.of(maybeCommit)));
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> nodes) {
		PriorityQueue<CommitEntry> queue = new PriorityQueue<>();
		ensureRepository(repositoryId); //ensuring repository to fetch from later
		return Promises.all(
				nodes.stream()
						.map(commitId -> tryGetCommit(repositoryId, commitId)
								.whenResult(maybeCommit -> maybeCommit.ifPresent(commit ->
										queue.add(new CommitEntry(commitId, commit))))))
				.map($ -> AsyncSupplier.cast(() -> {
					if (queue.isEmpty()) return Promise.of(null);
					CommitEntry entry = queue.poll();
					return Promises.all(
							entry.getCommit().getParents()
									.stream()
									.filter(nextCommitId -> !nextCommitId.isRoot() && queue.stream().map(CommitEntry::getCommitId).noneMatch(nextCommitId::equals))
									.map(nextCommitId -> tryGetCommit(repositoryId, nextCommitId)
											.whenResult(maybeNextCommit -> maybeNextCommit.ifPresent(nextCommit ->
													queue.add(new CommitEntry(nextCommitId, nextCommit))))))
							.map($2 -> entry);
				}))
				.map(ChannelSupplier::of)
				.whenComplete(toLogger(logger, FINEST, "download", repositoryId, this));
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId, Set<SignedData<RawCommitHead>> heads) {
		Set<CommitId> incomplete = new HashSet<>();
		return getIncompleteCommits(heads.stream().map(SignedData::getValue).map(RawCommitHead::getCommitId).collect(toSet()))
				.whenResult(incomplete::addAll)
				.map($ -> new AbstractChannelConsumer<CommitEntry>() {
					@Override
					protected Promise<Void> doAccept(@Nullable CommitEntry entry) {
						if (entry == null) {
							return Promise.complete();
						}
						if (incomplete.isEmpty()) {
							cancel();
							return Promise.ofException(CANCEL_EXCEPTION);
						}

						if (incomplete.remove(entry.getCommitId())) {
							return commitStorage.saveCommit(entry.getCommitId(), entry.getCommit()).toVoid()
									.then($ -> getIncompleteCommits(entry.getCommit().getParents())
											.then(parents -> {
												incomplete.addAll(parents);
												if (incomplete.isEmpty()) {
													return saveHeads(repositoryId, heads);
												}
												return Promise.complete();
											}));
						} else {
							return Promise.complete();
						}
					}
				})
				.map(consumer -> consumer
						.withAcknowledgement(ack -> ack
								.whenComplete(($, e) -> commitStorage.markCompleteCommits())));
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return commitStorage.saveSnapshot(encryptedSnapshot)
				.then(saved -> saved ?
						toMaster(repositoryId, node -> node.saveSnapshot(repositoryId, encryptedSnapshot)) :
						Promise.complete())
				.whenComplete(toLogger(logger, "saveSnapshot", repositoryId, encryptedSnapshot, this));
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		return commitStorage.loadSnapshot(repositoryId, commitId)
				.then(maybeSnapshot -> maybeSnapshot.isPresent() ?
						Promise.of(maybeSnapshot) :
						fromMaster(repositoryId,
								node -> node.loadSnapshot(repositoryId, commitId)
										.then(Promise::ofOptional),
								Promise.ofOptional(maybeSnapshot))
								.then(snapshot -> commitStorage.saveSnapshot(snapshot)
										.map($ -> snapshot))
								.mapEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.<SignedData<RawSnapshot>>empty()))
				.whenComplete(toLogger(logger, "loadSnapshot", repositoryId, commitId));
	}

	@Override
	public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId, Set<CommitId> remoteSnapshotIds) {
		return ensureRepository(repositoryId).ensureUpdated()
				.thenEx(($, e) -> commitStorage.listSnapshotIds(repositoryId)
						.then(localSnapshotIds -> {
							if (e == null || !localSnapshotIds.isEmpty()) {
								return Promise.of(difference(localSnapshotIds, remoteSnapshotIds));
							} else {
								return Promise.ofException(e);
							}
						}))
				.whenComplete(toLogger(logger, "listSnapshots", repositoryId, remoteSnapshotIds, this));
	}

	@Override
	public AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads(RepoID repositoryId) {
		return ensureRepository(repositoryId).pollHeads();
	}

	@Override
	public Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
		return ensureRepository(repositoryId).ensureUpdated()
				.thenEx(($, e) -> commitStorage.getHeads(repositoryId)
						.then(heads -> {
							if (e == null || !heads.isEmpty()) {
								return Promise.of(heads);
							} else {
								return Promise.ofException(e);
							}
						}))
				.map(map -> (Set<SignedData<RawCommitHead>>) new HashSet<>(map.values()))
				.whenComplete(toLogger(logger, "getHeads", repositoryId));
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return discoveryService.shareKey(receiver, simKey)
				.whenComplete(toLogger(logger, "shareKey", receiver, simKey, this));
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return discoveryService.getSharedKey(receiver, hash)
				.whenComplete(toLogger(logger, "getSharedKey", receiver, hash, this));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return discoveryService.getSharedKeys(receiver)
				.whenComplete(toLogger(logger, "getSharedKeys", receiver, this));
	}

	@Override
	public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		RepoID repository = pullRequest.getValue().getRepository();
		return commitStorage.savePullRequest(pullRequest)
				.then(saveStatus -> saveStatus ?
						toMaster(repository, node -> node.sendPullRequest(pullRequest)) :
						Promise.complete())
				.whenComplete(toLogger(logger, "sendPullRequest", pullRequest, this));
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return ensureRepository(repositoryId).ensureUpdated()
				.thenEx(($, e) -> commitStorage.getPullRequests(repositoryId)
						.then(pullRequests -> {
							if (e == null || !pullRequests.isEmpty()) {
								return Promise.of(pullRequests);
							} else {
								return Promise.ofException(e);
							}
						}))
				.whenComplete(toLogger(logger, "getPullRequests", repositoryId, this));
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get()
				.whenComplete(toLogger(logger, "catchUp", this));
	}

	private Promise<Void> doCatchUp() {
		return Promises.until(
				$1 -> {
					long timestampBegin = now.currentTimeMillis();
					return tolerantCollectVoid(Stream.<AsyncSupplier>of(this::fetch, this::update), AsyncSupplier::get)
							.map($2 -> now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());
				});
	}

	private Promise<Void> forEachRepository(Function<RepositoryEntry, Promise<Void>> fn) {
		return tolerantCollectVoid(namespaces.values().stream().flatMap(entry -> entry.getRepositories().values().stream()), fn);
	}

	private Promise<Void> update() {
		return tolerantCollectVoid(namespaces.values(), GlobalOTNamespace::updateRepositories)
				.thenEx(($, e) -> forEachRepository(RepositoryEntry::update));
	}

	public Promise<Void> fetch() {
		return forEachRepository(RepositoryEntry::fetch)
				.whenComplete(toLogger(logger, FINEST, "fetch", this));
	}

	public Promise<Void> push() {
		return forEachRepository(RepositoryEntry::push)
				.whenComplete(toLogger(logger, "push", this));
	}

	public Promise<Void> pushSnapshots() {
		return forEachRepository(RepositoryEntry::pushSnapshots)
				.whenComplete(toLogger(logger, "pushSnapshots", this));
	}

	public Promise<Void> pushPullRequests() {
		return forEachRepository(RepositoryEntry::pushPullRequests)
				.whenComplete(toLogger(logger, "pushPullRequests", this));
	}

	private <T> Promise<T> fromMaster(RepoID repositoryId, Function<GlobalOTNode, Promise<T>> fn, Promise<T> defaultPromise) {
		return ensureMasterNodes(repositoryId)
				.then(masters -> {
					if (isMasterFor(repositoryId.getOwner())) {
						return defaultPromise;
					}
					return firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() ->
									fn.apply(master))))
							.thenEx((v, e) -> e == null ? Promise.of(v) : defaultPromise);
				});
	}

	private Promise<Void> toMaster(RepoID repositoryId, Function<GlobalOTNode, Promise<Void>> fn) {
		return ensureMasterNodes(repositoryId)
				.then(masters -> {
					if (isMasterFor(repositoryId.getOwner())) {
						return Promise.complete();
					}
					return nSuccessesOrLess(propagations, masters.stream()
							.map(master -> AsyncSupplier.cast(() -> fn.apply(master))))
							.toVoid();
				});
	}

	private Promise<Set<CommitId>> getIncompleteCommits(Set<CommitId> commitIds) {
		Set<CommitId> incompleteCommits = new HashSet<>();
		return Promises.all(commitIds.stream().map(commitId -> commitStorage.isIncompleteCommit(commitId)
				.whenResult(isIncomplete -> {
					if (isIncomplete) {
						incompleteCommits.add(commitId);
					}
				})))
				.map($ -> incompleteCommits);
	}

	@Override
	public String toString() {
		return "GlobalOTNodeImpl{id=" + id + '}';
	}
}
