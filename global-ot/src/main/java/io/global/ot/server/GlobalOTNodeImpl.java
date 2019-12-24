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

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.async.service.EventloopService;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.Initializable;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.global.common.*;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.ot.api.*;
import io.global.ot.server.GlobalOTNamespace.RepositoryEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.promise.Promises.firstSuccessful;
import static io.global.util.Utils.tolerantCollectVoid;
import static io.global.util.Utils.untilTrue;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNodeImpl extends AbstractGlobalNode<GlobalOTNodeImpl, GlobalOTNamespace, GlobalOTNode> implements GlobalOTNode, EventloopService, Initializable<GlobalOTNodeImpl> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalOTNodeImpl.class);

	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.fixedDelay(Duration.ofSeconds(1)); //.immediateRetry().withMaxTotalRetryCount(10);
	public static final Boolean DEFAULT_POLL_MASTER_REPOSITORIES = ApplicationSettings.getBoolean(GlobalOTNodeImpl.class, "pollMasterRepositories", true);

	private final Eventloop eventloop;
	private final CommitStorage commitStorage;

	boolean pollMasterRepositories = DEFAULT_POLL_MASTER_REPOSITORIES;
	RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

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

	public Promise<Void> reset(RepoID repoID) {
		GlobalOTNamespace ns = namespaces.get(repoID.getOwner());
		return ns != null ? ns.reset(repoID) : Promise.complete();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		if (!pollMasterRepositories) {
			return Promise.complete();
		}
		pollMasterRepositories = false;
		namespaces.values()
				.stream()
				.flatMap(ns -> ns.getRepositories().values().stream())
				.forEach(RepositoryEntry::stopPolling);
		return Promise.complete();
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		GlobalOTNamespace ns = ensureNamespace(pubKey);
		return Promise.of(ns.getRepositories()
				.keySet()
				.stream()
				.map(RepoID::getName)
				.collect(toSet()));
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
										.whenResult(rawCommit -> commitStorage.saveCommit(commitId, rawCommit))
										.map(Optional::of),
								Promise.of(maybeCommit)));
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> nodes) {
		PriorityQueue<CommitEntry> queue = new PriorityQueue<>();
		ensureRepository(repositoryId);
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
				.whenComplete(toLogger(logger, TRACE, "download", repositoryId, this));
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId, Set<SignedData<RawCommitHead>> heads) {
		Set<CommitId> incomplete = new HashSet<>();
		ensureRepository(repositoryId);
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
		RepositoryEntry repositoryEntry = ensureRepository(repositoryId);
		return commitStorage.saveSnapshot(encryptedSnapshot)
				.map(saved -> {
					if (saved) {
						repositoryEntry.pushSnapshots();
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, "saveSnapshot", repositoryId, encryptedSnapshot, this));
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		ensureRepository(repositoryId);
		return commitStorage.loadSnapshot(repositoryId, commitId)
				// .then(maybeSnapshot -> maybeSnapshot.isPresent() ?
				// 		Promise.of(maybeSnapshot) :
				// 		fromMaster(repositoryId,
				// 				node -> node.loadSnapshot(repositoryId, commitId)
				// 						.then(Promise::ofOptional),
				// 				Promise.ofOptional(maybeSnapshot))
				// 				.whenResult(commitStorage::saveSnapshot)
				// 				.mapEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.<SignedData<RawSnapshot>>empty()))
				.whenComplete(toLogger(logger, "loadSnapshot", repositoryId, commitId));
	}

	@Override
	public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId) {
		ensureRepository(repositoryId);
		return commitStorage.listSnapshotIds(repositoryId)
				.whenComplete(toLogger(logger, "listSnapshots", repositoryId, this));
	}

	@Override
	public AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads(RepoID repositoryId) {
		return ensureRepository(repositoryId).pollHeads();
	}

	@Override
	public Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
		ensureRepository(repositoryId);
		return commitStorage.getHeads(repositoryId)
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
		RepositoryEntry repo = ensureRepository(pullRequest.getValue().getRepository());
		return commitStorage.savePullRequest(pullRequest)
				.map(saved -> {
					if (saved) {
						repo.pushPullRequests();
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, "sendPullRequest", pullRequest, this));
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		ensureRepository(repositoryId);
		return commitStorage.getPullRequests(repositoryId)
				.whenComplete(toLogger(logger, "getPullRequests", repositoryId, this));
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get()
				.whenComplete(toLogger(logger, "catchUp", this));
	}

	private Promise<Void> doCatchUp() {
		return untilTrue(() -> {
			long timestampBegin = now.currentTimeMillis();
			return tolerantCollectVoid(Stream.<AsyncSupplier>of(this::fetch, this::update), AsyncSupplier::get)
					.map($ -> now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());
		});
	}

	private Promise<Void> forEachRepository(Function<RepositoryEntry, Promise<Void>> fn) {
		return tolerantCollectVoid(namespaces.values().stream().flatMap(entry -> entry.getRepositories().values().stream()), fn);
	}

	public Promise<Void> update() {
		return tolerantCollectVoid(namespaces.values(), GlobalOTNamespace::updateRepositories)
				.thenEx(($, e) -> forEachRepository(RepositoryEntry::update))
				.whenComplete(toLogger(logger, TRACE, "update", this));
	}

	public Promise<Void> fetch() {
		return forEachRepository(RepositoryEntry::fetch)
				.whenComplete(toLogger(logger, TRACE, "fetch", this));
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
