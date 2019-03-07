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
import io.datakernel.exception.StacklessException;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.CollectorsEx;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.ot.api.*;
import io.global.ot.server.GlobalOTNodeImpl.PubKeyEntry.RepositoryEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.async.AsyncSuppliers.subscribe;
import static io.datakernel.async.Promises.asPromises;
import static io.datakernel.async.Promises.firstSuccessful;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.util.Utils.nSuccessesOrLess;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.Collections.emptySet;
import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toSet;

public final class GlobalOTNodeImpl implements GlobalOTNode, EventloopService, Initializable<GlobalOTNodeImpl> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalOTNodeImpl.class);

	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(GlobalOTNodeImpl.class, "latencyMargin", Duration.ofSeconds(20));

	private final RawServerId id;
	private final Eventloop eventloop;
	private final CommitStorage commitStorage;
	private final DiscoveryService discoveryService;
	private final NodeFactory<GlobalOTNode> nodeFactory;
	private final Set<PubKey> managedPubKeys = new HashSet<>();

	private final Map<PubKey, PubKeyEntry> pubKeys = new HashMap<>();

	private int propagations = 1;
	private int minimumSuccesses = 0;
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

	public RawServerId getId() {
		return id;
	}

	// Visible for testing
	PubKeyEntry ensurePubKey(PubKey pubKey) {
		return pubKeys.computeIfAbsent(pubKey, PubKeyEntry::new);
	}

	// Visible for testing
	RepositoryEntry ensureRepository(RepoID repositoryId) {
		return ensurePubKey(repositoryId.getOwner()).ensureRepository(repositoryId);
	}

	private Promise<List<GlobalOTNode>> ensureMasterNodes(PubKey pubKey) {
		return ensurePubKey(pubKey).ensureMasterNodes();
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
		return Promise.complete();
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		PubKeyEntry entry = ensurePubKey(pubKey);
		return entry
				.ensureMasterNodes()
				.thenCompose($ -> managedPubKeys.contains(pubKey) ?
						Promise.complete() :
						entry.updateRepositories())
				.thenApply($ -> entry.repositories
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
				.thenCompose(savedAny -> savedAny ?
						toMaster(repositoryId, node -> node.save(repositoryId, newCommits)) :
						Promise.complete())
				.whenComplete(toLogger(logger, "save", repositoryId, newCommits, this));
	}

	@Override
	public Promise<Void> updateHeads(RepoID repositoryId, Heads heads) {
		return commitStorage.updateHeads(repositoryId, heads.newHeads, heads.excludedHeads)
				.whenResult($ -> {
					if (!heads.newHeads.isEmpty()) ensureRepository(repositoryId).filterHeads();
				})
				.thenCompose($ -> toMaster(repositoryId, master -> master.updateHeads(repositoryId, heads)));
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return commitStorage.loadCommit(id)
				.thenCompose(maybeCommit -> maybeCommit.isPresent() ?
						Promise.ofOptional(maybeCommit) :
						fromMaster(repositoryId,
								node -> node.loadCommit(repositoryId, id)
										.thenCompose(commit -> commitStorage.saveCommit(id, commit)
												.thenApply($ -> commit)),
								Promise.ofOptional(maybeCommit)))
				.whenComplete(toLogger(logger, "loadCommit", repositoryId, id, this));
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> required, Set<CommitId> existing) {
		checkArgument(!hasIntersection(required, existing), "Required heads and existing heads cannot have intersections");
		Set<CommitId> skipCommits = new HashSet<>(existing);
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		ensureRepository(repositoryId); //ensuring repository to fetch from later
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Promises.all(
						union(thisHeads.keySet(), required, existing)
								.stream()
								.map(commitId -> commitStorage.loadCommit(commitId)
										.thenCompose(maybeCommit -> maybeCommit.isPresent() ?
												Promise.of(maybeCommit) :
												fromMaster(repositoryId, node -> node.loadCommit(repositoryId, commitId)
																.thenCompose(rawCommit -> commitStorage.saveCommit(commitId, rawCommit)
																		.thenApply($ -> Optional.of(rawCommit))),
														Promise.of(maybeCommit)))
										.whenResult(maybeCommit -> maybeCommit.ifPresent(commit ->
												queue.add(new RawCommitEntry(commitId, commit))))))
						.thenApply($ -> AsyncSupplier.cast(() -> getNextStreamEntry(repositoryId, queue, skipCommits, required, existing)))
						.thenApply(ChannelSupplier::of)
						.thenApply(supplier -> supplier.map(
								entry -> new CommitEntry(entry.commitId, entry.commit, thisHeads.get(entry.commitId))))
				)
				.whenComplete(toLogger(logger, TRACE, "download", repositoryId, required, existing, this));
	}

	private Promise<RawCommitEntry> getNextStreamEntry(RepoID repositoryId, PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> required, Set<CommitId> existing) {
		return Promise.ofCallback(cb -> getNextStreamEntryImpl(repositoryId, queue, skipCommits, required, existing, cb));
	}

	private void getNextStreamEntryImpl(RepoID repositoryId, PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> required, Set<CommitId> existing,
			SettableCallback<RawCommitEntry> cb) {
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
								.thenCompose(maybeNextCommit -> maybeNextCommit.isPresent() ?
										Promise.of(maybeNextCommit) :
										fromMaster(repositoryId, node -> node.loadCommit(repositoryId, nextCommitId)
														.thenCompose(rawCommit -> commitStorage.saveCommit(nextCommitId, rawCommit)
																.thenApply($ -> Optional.of(rawCommit))),
												Promise.of(maybeNextCommit))
								)
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
					getNextStreamEntryImpl(repositoryId, queue, skipCommits, required, existing, cb);
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		return ensureMasterNodes(repositoryId)
				.thenCompose(masters -> isMasterFor(repositoryId) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.updateHeads())
				.thenCompose($ -> getLocalHeadsInfo(repositoryId))
				.whenComplete(toLogger(logger, "getHeadsInfo", repositoryId, this));
	}

	Promise<HeadsInfo> getLocalHeadsInfo(RepoID repositoryId) {
		Set<CommitId> existing = new HashSet<>();
		Set<CommitId> required = new HashSet<>();
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
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
						.thenCompose($ -> Promise.ofCallback((SettableCallback<Set<CommitId>> cb) ->
								findMissingParents(queue, new HashSet<>(), cb)))
						.whenResult(required::addAll)
						.thenApply($ -> new HeadsInfo(existing, required)));
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
		return ensureMasterNodes(repositoryId)
				.thenCompose(nodes -> {
					if (isMasterFor(repositoryId)) {
						return uploadLocal(repositoryId);
					}
					return nSuccessesOrLess(propagations, nodes
							.stream()
							.map(node -> AsyncSupplier.cast(() -> node.upload(repositoryId))))
							.thenApply(consumers -> {
								ChannelZeroBuffer<CommitEntry> buffer = new ChannelZeroBuffer<>();

								ChannelSplitter<CommitEntry> splitter = ChannelSplitter.create(buffer.getSupplier())
										.lenient();

								boolean[] localCompleted = {false};
								splitter.addOutput()
										.set(ChannelConsumer.ofPromise(uploadLocal(repositoryId))
												.withAcknowledgement(ack -> ack
														.whenComplete(($, e) -> {
															if (e == null) {
																localCompleted[0] = true;
															} else {
																splitter.close(e);
															}
														})));

								int[] up = {consumers.size()};

								consumers.forEach(output -> splitter.addOutput()
										.set(output
												.withAcknowledgement(ack -> ack.whenException(e -> {
													if (e != null && --up[0] < minimumSuccesses && localCompleted[0]) {
														splitter.close(e);
													}
												}))));

								MaterializedPromise<Void> process = splitter.startProcess();

								return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process)
										.thenCompose($ -> {
											if (up[0] >= minimumSuccesses) {
												return Promise.complete();
											}
											return Promise.ofException(new StacklessException(GlobalOTNodeImpl.class, "Not enough successes"));
										})
								);
							});
				});
	}

	private Promise<ChannelConsumer<CommitEntry>> uploadLocal(RepoID repositoryId) {
		Set<CommitId> excludedHeads = new HashSet<>();
		Set<SignedData<RawCommitHead>> addedHeads = new HashSet<>();
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenApply(heads ->
						ChannelConsumer.of(
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
										.thenCompose($ -> updateHeads(repositoryId, new Heads(addedHeads, excludedHeads))))
				);
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return commitStorage.saveSnapshot(encryptedSnapshot)
				.thenCompose(saved -> saved ?
						toMaster(repositoryId, node -> node.saveSnapshot(repositoryId, encryptedSnapshot)) :
						Promise.complete())
				.whenComplete(toLogger(logger, "saveSnapshot", repositoryId, encryptedSnapshot, this));
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		return commitStorage.loadSnapshot(repositoryId, commitId)
				.thenCompose(maybeSnapshot -> maybeSnapshot.isPresent() ?
						Promise.of(maybeSnapshot) :
						fromMaster(repositoryId,
								node -> node.loadSnapshot(repositoryId, commitId)
										.thenCompose(Promise::ofOptional),
								Promise.ofOptional(maybeSnapshot))
								.thenCompose(snapshot -> commitStorage.saveSnapshot(snapshot)
										.thenApply($ -> snapshot))
								.thenApplyEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.<SignedData<RawSnapshot>>empty()))
				.whenComplete(toLogger(logger, "loadSnapshot", repositoryId, commitId));
	}

	@Override
	public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId, Set<CommitId> remoteSnapshotIds) {
		return ensureMasterNodes(repositoryId)
				.thenCompose(masters -> isMasterFor(repositoryId) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.updateSnapshots())
				.thenCompose($ -> commitStorage.listSnapshotIds(repositoryId))
				.thenApply(localSnapshotIds -> difference(localSnapshotIds, remoteSnapshotIds))
				.whenComplete(toLogger(logger, "listSnapshots", repositoryId, remoteSnapshotIds, this));
	}

	@Override
	public Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
		return ensureMasterNodes(repositoryId)
				.thenCompose(masters -> isMasterFor(repositoryId) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.updateHeads())
				.thenCompose($ -> commitStorage.getHeads(repositoryId))
				.thenCompose(heads -> Promise.of(new Heads(
						difference(heads.keySet(), remoteHeads)
								.stream()
								.map(heads::get)
								.collect(toSet()), emptySet()))
				)
				.whenComplete(toLogger(logger, "getHeads", repositoryId, remoteHeads));
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
				.thenApply(filteredHeads -> difference(heads, filteredHeads))
				.whenComplete(toLogger(logger, "excludeParents", heads, this));
	}

	private void doExcludeParents(PriorityQueue<RawCommitEntry> queue, long minLevel,
			Set<CommitId> resultHeads,
			SettableCallback<Set<CommitId>> cb) {
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
				.thenCompose(saveStatus -> saveStatus ?
						toMaster(repository, node -> node.sendPullRequest(pullRequest)) :
						Promise.complete())
				.whenComplete(toLogger(logger, "sendPullRequest", pullRequest, this));
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return ensureMasterNodes(repositoryId)
				.thenCompose(masters -> isMasterFor(repositoryId) ?
						Promise.complete() :
						ensureRepository(repositoryId)
								.updatePullRequests())
				.thenCompose($ -> commitStorage.getPullRequests(repositoryId))
				.whenComplete(toLogger(logger, "getPullRequests", repositoryId, this));
	}

	private final AsyncSupplier<Void> catchUp = reuse(() -> Promise.ofCallback(this::catchUpImpl));

	public Promise<Void> catchUp() {
		return catchUp.get()
				.whenComplete(toLogger(logger, "catchUp", this));
	}

	private void catchUpImpl(SettableCallback<Void> cb) {
		long timestampBegin = now.currentTimeMillis();
		Promise<Void> fetchPromise = fetch();
		if (fetchPromise.isResult()) {
			cb.set(fetchPromise.materialize().getResult());
		} else if (fetchPromise.isException()) {
			cb.setException(fetchPromise.materialize().getException());
		} else {
			fetchPromise
					.whenResult($ -> {
						long timestampEnd = now.currentTimeMillis();
						if (timestampEnd - timestampBegin > latencyMargin.toMillis()) {
							catchUpImpl(cb);
						} else {
							cb.set(null);
						}
					})
					.whenException(cb::setException);
		}
	}

	private Promise<Void> forEachRepository(Function<RepositoryEntry, Promise<Void>> fn) {
		return tolerantCollectVoid(pubKeys.values().stream().flatMap(entry -> entry.repositories.values().stream()), fn);
	}

	public Promise<Void> fetch() {
		return forEachRepository(RepositoryEntry::fetch)
				.whenComplete(toLogger(logger, TRACE, "fetch", this));
	}

	public Promise<Void> update() {
		return tolerantCollectVoid(pubKeys.values(), PubKeyEntry::updateRepositories)
				.thenComposeEx(($, e) -> forEachRepository(RepositoryEntry::update));
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

	private boolean isMasterFor(RepoID repositoryId) {
		return managedPubKeys.contains(repositoryId.getOwner());
	}

	private <T> Promise<T> fromMaster(RepoID repositoryId, Function<GlobalOTNode, Promise<T>> fn, Promise<T> defaultPromise) {
		return ensureMasterNodes(repositoryId)
				.thenCompose(masters -> {
					if (isMasterFor(repositoryId)) {
						return defaultPromise;
					}
					return firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() ->
									fn.apply(master))))
							.thenComposeEx((v, e) -> e == null ? Promise.of(v) : defaultPromise);
				});
	}

	private Promise<Void> toMaster(RepoID repositoryId, Function<GlobalOTNode, Promise<Void>> fn) {
		return ensureMasterNodes(repositoryId)
				.thenCompose(masters -> {
					if (isMasterFor(repositoryId)) {
						return Promise.complete();
					}
					return nSuccessesOrLess(propagations, masters
							.stream()
							.map(master -> AsyncSupplier.cast(() -> fn.apply(master))))
							.toVoid();
				});
	}

	@Override
	public String toString() {
		return "GlobalOTNodeImpl{id=" + id + '}';
	}

	class PubKeyEntry {
		private final PubKey pubKey;
		private final Map<RepoID, RepositoryEntry> repositories = new HashMap<>();

		// Visible for testing
		final Map<RawServerId, GlobalOTNode> masterNodes = new HashMap<>();
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

		@NotNull
		Promise<List<GlobalOTNode>> doEnsureMasterNodes() {
			if (updateNodesTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.of(getMasterNodes());
			}
			return discoveryService.find(pubKey)
					.thenApplyEx((announceData, e) -> {
						if (e == null) {
							AnnounceData announce = announceData.getValue();
							if (announce.getTimestamp() >= announceTimestamp) {
								Set<RawServerId> newServerIds = new HashSet<>(announce.getServerIds());
								masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
								if (newServerIds.remove(id)) { // ensure that we are master for the pubKey if it was announced
									if (managedPubKeys.add(pubKey)) {
										logger.trace("became a master for {}: {}", pubKey, GlobalOTNodeImpl.this);
									}
								} else {
									if (managedPubKeys.remove(pubKey)) {
										logger.trace("stopped being a master for {}: {}", pubKey, GlobalOTNodeImpl.this);
									}
								}
								newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, nodeFactory::create));
								updateNodesTimestamp = now.currentTimeMillis();
								announceTimestamp = announce.getTimestamp();
							}
						}
						return getMasterNodes();
					});
		}

		@NotNull
		private Promise<Void> doUpdateRepositories() {
			logger.trace("Updating repositories");
			if (updateRepositoriesTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.complete();
			}
			return ensureMasterNodes()
					.thenCompose(masters -> firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() ->
									master.list(pubKey))))
							.thenComposeEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptySet())))
					.whenResult(repoNames -> repoNames.forEach(name -> ensureRepository(RepoID.of(pubKey, name))))
					.whenResult($ -> updateRepositoriesTimestamp = now.currentTimeMillis())
					.toVoid();
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
			private long updateSnapshotsTimestamp;
			private long updatePullRequestsTimestamp;

			private final AsyncSupplier<Void> update = reuse(this::doUpdate);
			private final AsyncSupplier<Void> updateHeads = reuse(this::doUpdateHeads);
			private final AsyncSupplier<Void> updateSnapshots = reuse(this::doUpdateSnapshots);
			private final AsyncSupplier<Void> updatePullRequests = reuse(this::doUpdatePullRequests);
			private final AsyncSupplier<Void> filterHeads = subscribe(this::doFilterHeads);
			private final AsyncSupplier<Void> fetch = reuse(this::doFetch);
			private final AsyncSupplier<Void> push = reuse(this::doPush);
			private final AsyncSupplier<Void> pushSnapshots = reuse(this::doPushSnapshots);
			private final AsyncSupplier<Void> pushPullRequests = reuse(this::doPushPullRequests);

			RepositoryEntry(RepoID repositoryId) {
				this.repositoryId = repositoryId;
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

			public Promise<Void> filterHeads() {
				return filterHeads.get();
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

			@NotNull
			private Promise<Void> doUpdate() {
				if (updateTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return Promises.all(updateHeads(), updatePullRequests(), updateSnapshots())
						.whenResult($ -> updateTimestamp = now.currentTimeMillis());
			}

			@NotNull
			private Promise<Void> doUpdateHeads() {
				logger.trace("Updating heads");
				if (updateHeadsTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return ensureMasterNodes()
						.thenCompose(masters -> commitStorage.getHeads(repositoryId)
								.thenCompose(heads -> firstSuccessful(masters.stream()
										.map(master -> AsyncSupplier.cast(() ->
												master.getHeads(repositoryId, heads.keySet()))))
										.thenComposeEx((v, e) -> Promise.of(e == null ? v : new Heads(emptySet(), emptySet()))))
								.thenCompose(headsDelta ->
										commitStorage.updateHeads(repositoryId, headsDelta.newHeads, headsDelta.excludedHeads)
												.thenCompose($ -> headsDelta.newHeads.isEmpty() ?
														Promise.complete() :
														filterHeads())))
						.whenResult($ -> updateHeadsTimestamp = now.currentTimeMillis());
			}

			@NotNull
			private Promise<Void> doUpdateSnapshots() {
				logger.trace("Updating snapshots");
				if (updateSnapshotsTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return ensureMasterNodes()
						.thenCompose(masters -> commitStorage.listSnapshotIds(repositoryId)
								.thenCompose(localSnapshotIds -> firstSuccessful(masters.stream()
										.map(master -> AsyncSupplier.cast(() ->
												master.listSnapshots(repositoryId, localSnapshotIds)
														.thenCompose(newSnapshotIds -> Promises.toList(
																newSnapshotIds.stream()
																		.map(snapshotId -> master.loadSnapshot(repositoryId, snapshotId)
																				.thenCompose(Promise::ofOptional)))))))
										.thenComposeEx((v, e) -> Promise.of(e == null ? v : Collections.<SignedData<RawSnapshot>>emptyList())))
								.thenCompose(snapshots -> Promises.all(snapshots.stream().map(commitStorage::saveSnapshot))))
						.whenResult($ -> updateSnapshotsTimestamp = now.currentTimeMillis());
			}

			@NotNull
			private Promise<Void> doUpdatePullRequests() {
				logger.trace("Updating pull requests");
				if (updatePullRequestsTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.complete();
				}
				return ensureMasterNodes()
						.thenCompose(masters -> firstSuccessful(masters.stream()
								.map(master -> AsyncSupplier.cast(() ->
										master.getPullRequests(repositoryId))))
								.thenComposeEx((v, e) -> Promise.of(e == null ? v : Collections.<SignedData<RawPullRequest>>emptySet())))
						.thenCompose(pullRequests -> Promises.all(
								pullRequests.stream().map(commitStorage::savePullRequest)))
						.whenResult($ -> updatePullRequestsTimestamp = now.currentTimeMillis());
			}

			@NotNull
			private Promise<Void> doFilterHeads() {
				logger.trace("Filtering heads");
				return commitStorage.getHeads(repositoryId)
						.thenCompose(heads -> excludeParents(heads.keySet())
								.thenCompose(excludedHeadIds -> commitStorage.updateHeads(repositoryId, emptySet(), excludedHeadIds)));
			}

			private Promise<Void> forEachMaster(Function<GlobalOTNode, Promise<Void>> action) {
				return ensureMasterNodes()
						.thenCompose(masters -> tolerantCollectVoid(masters, action));
			}

			@NotNull
			private Promise<Void> doFetch() {
				return forEachMaster(master -> {
					logger.trace("{} fetching from {}", repositoryId, master);
					return getLocalHeadsInfo(repositoryId)
							.thenCompose(headsInfo -> ChannelSupplier.ofPromise(master.download(repositoryId, headsInfo.getRequired(), headsInfo.getExisting()))
									.streamTo(ChannelConsumer.ofPromise(uploadLocal(repositoryId))));
				});
			}

			@NotNull
			private Promise<Void> doPush() {
				return forEachMaster(master -> {
					logger.trace("{} pushing to {}", repositoryId, master);
					return master.getHeadsInfo(repositoryId)
							.thenCompose(headsInfo -> ChannelSupplier.ofPromise(download(repositoryId, headsInfo.getRequired(), headsInfo.getExisting()))
									.streamTo(ChannelConsumer.ofPromise(master.upload(repositoryId))));
				});
			}

			@NotNull
			private Promise<Void> doPushSnapshots() {
				return forEachMaster(master -> {
					logger.trace("{} pushing snapshots to {}", repositoryId, master);
					//noinspection OptionalGetWithoutIsPresent - snapshot presence is checked in commitStorage.listSnapshotIds()
					return master.listSnapshots(repositoryId, emptySet())
							.thenCompose(remoteSnapshotIds -> commitStorage.listSnapshotIds(repositoryId)
									.thenApply(localSnapshotIds -> difference(localSnapshotIds, remoteSnapshotIds)))
							.thenCompose(snapshotsIds -> Promises.toList(snapshotsIds.stream()
									.map(snapshot -> commitStorage.loadSnapshot(repositoryId, snapshot))))
							.thenCompose(snapshots -> tolerantCollectVoid(snapshots,
									snapshot -> master.saveSnapshot(repositoryId, snapshot.get())));
				});
			}

			@NotNull
			private Promise<Void> doPushPullRequests() {
				return forEachMaster(master -> {
					logger.trace("{} pushing pull requests to {}", repositoryId, master);
					return master.getPullRequests(repositoryId)
							.thenCompose(remotePullRequests -> commitStorage.getPullRequests(repositoryId)
									.thenApply(localPullRequests -> difference(localPullRequests, remotePullRequests)))
							.thenCompose(pullRequests -> tolerantCollectVoid(pullRequests, master::sendPullRequest));
				});
			}
		}
	}
}
