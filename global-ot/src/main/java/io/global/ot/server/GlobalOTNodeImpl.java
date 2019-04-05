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
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.CollectorsEx;
import io.datakernel.util.Initializable;
import io.datakernel.util.ref.RefBoolean;
import io.global.common.*;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.ot.api.*;
import io.global.ot.server.GlobalOTNamespace.RepositoryEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.async.Promises.asPromises;
import static io.datakernel.async.Promises.firstSuccessful;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.util.Utils.nSuccessesOrLess;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNodeImpl extends AbstractGlobalNode<GlobalOTNodeImpl, GlobalOTNamespace, GlobalOTNode> implements GlobalOTNode, EventloopService, Initializable<GlobalOTNodeImpl> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalOTNodeImpl.class);

	public static final Boolean DEFAULT_POLL_MASTER_REPOSITORIES = ApplicationSettings.getBoolean(GlobalOTNodeImpl.class, "pollMasterRepositories", true);

	private final Eventloop eventloop;
	private final CommitStorage commitStorage;

	boolean pollMasterRepositories = DEFAULT_POLL_MASTER_REPOSITORIES;

	private int propagations = 1;
	private int minimumSuccesses = 0;

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
		pollMasterRepositories = false;
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
		return Promises.reduce(CollectorsEx.toAny(), 1,
				asPromises(newCommits.entrySet().stream().map(entry -> AsyncSupplier.cast(() ->
						commitStorage.saveCommit(entry.getKey(), entry.getValue())
				))))
				.then(savedAny -> savedAny ?
						toMaster(repositoryId, node -> node.save(repositoryId, newCommits)) :
						Promise.complete())
				.whenComplete(toLogger(logger, "save", repositoryId, newCommits, this));
	}

	@Override
	public Promise<Void> saveHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads) {
		if (newHeads.isEmpty()) {
			return Promise.complete();
		}
		return ensureRepository(repositoryId).saveHeads(newHeads)
				.then($ -> toMaster(repositoryId, master -> master.saveHeads(repositoryId, newHeads)));
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return commitStorage.loadCommit(id)
				.then(maybeCommit -> maybeCommit.isPresent() ?
						Promise.ofOptional(maybeCommit) :
						fromMaster(repositoryId,
								node -> node.loadCommit(repositoryId, id)
										.then(commit -> commitStorage.saveCommit(id, commit)
												.map($ -> commit)),
								Promise.ofOptional(maybeCommit)))
				.whenComplete(toLogger(logger, "loadCommit", repositoryId, id, this));
	}

	private Promise<Optional<RawCommit>> getCommit(RepoID repositoryId, CommitId commitId) {
		return commitStorage.loadCommit(commitId)
				.then(maybeCommit -> maybeCommit.isPresent() ?
						Promise.of(maybeCommit) :
						fromMaster(repositoryId, node -> node.loadCommit(repositoryId, commitId)
										.then(rawCommit -> commitStorage.saveCommit(commitId, rawCommit)
												.map($ -> Optional.of(rawCommit))),
								Promise.of(maybeCommit)));
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> required, Set<CommitId> existing) {
		checkArgument(!hasIntersection(required, existing), "Required heads and existing heads cannot have intersections");
		Set<CommitId> skipCommits = new HashSet<>(existing);
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		ensureRepository(repositoryId); //ensuring repository to fetch from later
		return commitStorage.getHeads(repositoryId)
				.then(thisHeads -> Promises.all(
						union(thisHeads.keySet(), required, existing).stream()
								.map(commitId -> getCommit(repositoryId, commitId)
										.whenResult((Consumer<? super Optional<RawCommit>>) maybeCommit -> maybeCommit.ifPresent(commit ->
												queue.add(new RawCommitEntry(commitId, commit))))))
						.map($ -> AsyncSupplier.cast(() -> getNextStreamEntry(repositoryId, queue, skipCommits, required, existing)))
						.map(ChannelSupplier::of)
						.map(supplier -> supplier.map(
								entry -> new CommitEntry(entry.commitId, entry.commit, thisHeads.get(entry.commitId))))
				)
				.whenComplete(toLogger(logger, TRACE, "download", repositoryId, required, existing, this));
	}

	private Promise<@Nullable RawCommitEntry> getNextStreamEntry(RepoID repositoryId, PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> required, Set<CommitId> existing) {
		return Promise.ofCallback(cb -> getNextStreamEntryImpl(repositoryId, queue, skipCommits, required, existing, cb));
	}

	private void getNextStreamEntryImpl(RepoID repositoryId, PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> required, Set<CommitId> existing,
			SettableCallback<@Nullable RawCommitEntry> cb) {
		if (queue.isEmpty() || queue.stream().map(RawCommitEntry::getCommitId).allMatch(skipCommits::contains)) {
			cb.set(null);
			return;
		}
		RawCommitEntry entry = queue.poll();
		boolean skipped = skipCommits.remove(entry.commitId);
		Set<CommitId> nextCommitIds = entry.getCommit().getParents();
		Promises.all(
				nextCommitIds.stream()
						.filter(nextCommitId -> !existing.contains(nextCommitId))
						.filter(nextCommitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(nextCommitId::equals))
						.map(nextCommitId -> getCommit(repositoryId, nextCommitId)
								.whenResult((Consumer<? super Optional<RawCommit>>) maybeNextCommit -> maybeNextCommit.ifPresent(nextCommit -> {
									if (skipped && !required.contains(nextCommitId)) {
										skipCommits.add(nextCommitId);
									}
									queue.add(new RawCommitEntry(nextCommitId, nextCommit));
								}))))
				.whenResult((Consumer<? super Void>) $ -> {
					if (!skipped) {
						cb.set(entry);
						return;
					}
					getNextStreamEntryImpl(repositoryId, queue, skipCommits, required, existing, cb);
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		return ensureMasterNodes(repositoryId)
				.then(masters -> isMasterFor(repositoryId.getOwner()) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.update())
				.then($ -> getLocalHeadsInfo(repositoryId))
				.whenComplete(toLogger(logger, "getHeadsInfo", repositoryId, this));
	}

	Promise<HeadsInfo> getLocalHeadsInfo(RepoID repositoryId) {
		Set<CommitId> existing = new HashSet<>();
		Set<CommitId> required = new HashSet<>();
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.map(Map::keySet)
				.then(heads -> Promises.all(
						heads.stream()
								.map(headId -> commitStorage.loadCommit(headId)
										.whenResult((Consumer<? super Optional<RawCommit>>) maybeCommit -> {
											if (maybeCommit.isPresent()) {
												existing.add(headId);
												queue.add(new RawCommitEntry(headId, maybeCommit.get()));
											} else {
												required.add(headId);
											}
										})))
						.then($ -> Promise.ofCallback((SettableCallback<Set<CommitId>> cb) ->
								findMissingParents(queue, new HashSet<>(), cb)))
						.whenResult((Consumer<? super Set<CommitId>>) required::addAll)
						.map($ -> new HeadsInfo(existing, required)));
	}

	private void findMissingParents(PriorityQueue<RawCommitEntry> queue, Set<CommitId> missingParents, SettableCallback<Set<CommitId>> cb) {
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
								.then(isCompleteCommit -> isCompleteCommit ?
										Promise.complete() :
										commitStorage.loadCommit(parentId)
												.whenResult((Consumer<? super Optional<RawCommit>>) maybeCommit -> {
													if (maybeCommit.isPresent()) {
														queue.add(new RawCommitEntry(parentId, maybeCommit.get()));
													} else {
														missingParents.add(parentId);
													}
												})
												.toVoid())))
				.whenResult((Consumer<? super Void>) $ -> findMissingParents(queue, missingParents, cb))
				.whenException(cb::setException);
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId) {
		return ensureMasterNodes(repositoryId)
				.then(nodes -> {
					if (isMasterFor(repositoryId.getOwner())) {
						return uploadLocal(repositoryId);
					}
					return nSuccessesOrLess(propagations, nodes
							.stream()
							.map(node -> AsyncSupplier.cast(() -> node.upload(repositoryId))))
							.map(consumers -> {
								ChannelZeroBuffer<CommitEntry> buffer = new ChannelZeroBuffer<>();
								ChannelSplitter<CommitEntry> splitter = ChannelSplitter.create(buffer.getSupplier()).lenient();

								RefBoolean localCompleted = new RefBoolean(false);
								splitter.addOutput()
										.set(ChannelConsumer.ofPromise(uploadLocal(repositoryId))
												.withAcknowledgement(ack -> ack
														.whenComplete((Callback<? super Void>) ($, e) -> {
															if (e == null) {
																localCompleted.set(true);
															} else {
																splitter.close(e);
															}
														})));

								MaterializedPromise<Void> process = splitter.splitInto(consumers, minimumSuccesses, localCompleted);
								return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
							});
				});
	}

	public Promise<ChannelConsumer<CommitEntry>> uploadLocal(RepoID repositoryId) {
		Set<SignedData<RawCommitHead>> newHeads = new HashSet<>();
		return commitStorage.getHeads(repositoryId)
				.map(Map::keySet)
				.map(heads ->
						ChannelConsumer.of(
								(CommitEntry entry) -> {
									if (entry.hasHead()) {
										newHeads.add(entry.getHead());
									}
									return commitStorage.saveCommit(entry.commitId, entry.commit).toVoid();
								})
								.withAcknowledgement(ack -> ack
										.then($ -> commitStorage.markCompleteCommits())
										.then($ -> ensureRepository(repositoryId).saveHeads(newHeads))
								)
				);
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
		return ensureMasterNodes(repositoryId)
				.then($ -> isMasterFor(repositoryId.getOwner()) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.updateSnapshots())
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
		return ensureMasterNodes(repositoryId)
				.then($ -> isMasterFor(repositoryId.getOwner()) ?
						Promise.complete() :
						ensureRepository(repositoryId).update())
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
		return ensureMasterNodes(repositoryId)
				.then($ -> isMasterFor(repositoryId.getOwner()) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.updatePullRequests())
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
					return fetch()
							.map($2 ->
									now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());

				});
	}

	private Promise<Void> forEachRepository(Function<RepositoryEntry, Promise<Void>> fn) {
		return tolerantCollectVoid(namespaces.values().stream().flatMap(entry -> entry.getRepositories().values().stream()), fn);
	}

	public Promise<Void> fetch() {
		return forEachRepository(RepositoryEntry::fetch)
				.whenComplete(toLogger(logger, TRACE, "fetch", this));
	}

	public Promise<Void> update() {
		return tolerantCollectVoid(namespaces.values(), GlobalOTNamespace::updateRepositories)
				.thenEx(($, e) -> forEachRepository(RepositoryEntry::update));
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

	@Override
	public String toString() {
		return "GlobalOTNodeImpl{id=" + id + '}';
	}
}
