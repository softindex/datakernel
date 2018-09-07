package io.global.globalsync.server;

import io.datakernel.async.*;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.SignedData;
import io.global.globalsync.api.*;
import io.global.globalsync.api.RawServer.CommitEntry;
import io.global.globalsync.api.RawServer.HeadsDelta;
import io.global.globalsync.api.RawServer.HeadsInfo;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.resubscribe;
import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.emptySet;
import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toSet;

public final class RawServer_Repository {
	public static final int DOWNLOAD_PREFETCH_COUNT = 10;
	private final CommitStorage commitStorage;
	private final RepositoryName repositoryId;
	private final AsyncSupplier<List<RawServer>> ensureServers;
	private final Settings settings;

	public interface Settings {
		Duration getLatencyMargin();
	}

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	long updateTimestamp;

	private final AsyncSupplier<Void> update = reuse(this::update);
	private final AsyncSupplier<Void> catchUp = reuse(this::catchUp);
	private final AsyncSupplier<Void> filterHeads = resubscribe(this::doFilterHeads);

	RawServer_Repository(CommitStorage commitStorage, AsyncSupplier<List<RawServer>> ensureServers,
			RepositoryName repositoryId,
			Settings settings) {
		this.commitStorage = commitStorage;
		this.ensureServers = ensureServers;
		this.repositoryId = repositoryId;
		this.settings = settings;
	}

	public Stage<Void> update() {
		if (updateTimestamp >= now.currentTimeMillis() - settings.getLatencyMargin().toMillis()) {
			return Stage.complete();
		}
		return Stages.all(updateHeads(), updatePullRequests())
				.thenRun(() -> updateTimestamp = now.currentTimeMillis());
	}

	private Stage<Void> updateHeads() {
		return Stages.toTuple(ServersAndHeads::new, ensureServers(), commitStorage.getHeads(repositoryId))
				.thenCompose(tuple -> Stages.firstSuccessful(
						tuple.servers.stream()
								.map(server ->
										server.getHeads(repositoryId, tuple.heads.keySet()))))
				.thenCompose(headsDelta ->
						commitStorage.updateHeads(repositoryId, headsDelta.newHeads, headsDelta.excludedHeads));
	}

	public Stage<Void> updatePullRequests() {
		return ensureServers()
				.thenCompose(servers -> Stages.firstSuccessful(
						servers.stream()
								.map(server -> server.getPullRequests(repositoryId))))
				.thenCompose(pullRequests -> Stages.all(
						pullRequests.stream()
								.map(commitStorage::savePullRequest)))
				.toVoid();
	}

	public Stage<Void> sync() {
		return Stages.all(catchUp.get(), updatePullRequests())
				.whenResult($ -> updateTimestamp = now.currentTimeMillis());
	}

	private Stage<Void> catchUp() {
		return Stage.ofCallback(this::doCatchUp);
	}

	private void doCatchUp(Callback<Void> cb) {
		long timestampBegin = now.currentTimeMillis();
		fetch()
				.thenCompose($ -> commitStorage.markCompleteCommits())
				.thenRun(() -> {
					long timestampEnd = now.currentTimeMillis();
					if (timestampEnd - timestampBegin > settings.getLatencyMargin().toMillis()) {
						cb.set(null);
					} else {
						doCatchUp(cb);
					}
				})
				.whenException(cb::setException);
	}

	public Stage<Set<SignedData<RawPullRequest>>> getPullRequests() {
		return update()
				.thenCompose($ -> commitStorage.getPullRequests(this.repositoryId));
	}

	public Stage<List<RawServer>> ensureServers() {
		return ensureServers.get();
	}

	public Stage<HeadsDelta> getHeads(Set<CommitId> remoteHeads) {
		return commitStorage.getHeads(repositoryId)
				.thenCompose(heads -> excludeParents(union(heads.keySet(), remoteHeads))
						.thenApply(excludedHeads -> new HeadsDelta(
								difference(heads.keySet(), union(remoteHeads, excludedHeads)).stream()
										.map(heads::get)
										.collect(toSet()),
								excludedHeads)));
	}

	private Stage<Void> doFilterHeads() {
		return commitStorage.getHeads(repositoryId)
				.thenCompose(heads -> excludeParents(heads.keySet())
						.thenCompose(excludedHeadIds -> commitStorage.updateHeads(repositoryId, emptySet(), excludedHeadIds)))
				.toVoid();
	}

	public Stage<Set<CommitId>> excludeParents(Set<CommitId> heads) {
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return Stages.all(heads.stream()
				.map(head -> commitStorage.loadCommit(head)
						.whenResult(optional -> optional.ifPresent(rawCommit ->
								queue.add(new RawCommitEntry(head, rawCommit))))))
				.thenCallback((Void $, Callback<Set<CommitId>> cb) ->
						doExcludeParents(
								queue,
								queue.stream().mapToLong(entry -> entry.rawCommit.getLevel()).min().orElse(0L),
								new HashSet<>(heads),
								cb))
				.thenApply(filteredHeads -> difference(heads, filteredHeads));
	}

	private void doExcludeParents(PriorityQueue<RawCommitEntry> queue, long minLevel,
			Set<CommitId> resultHeads,
			Callback<Set<CommitId>> cb) {
		RawCommitEntry entry = queue.poll();
		if (entry == null || entry.rawCommit.getLevel() < minLevel) {
			cb.set(resultHeads);
			return;
		}
		resultHeads.removeAll(entry.rawCommit.getParents());
		Stages.all(
				entry.rawCommit.getParents().stream()
						.filter(commitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(commitId::equals))
						.map(parentId ->
								commitStorage.loadCommit(parentId)
										.whenResult(optional -> optional.ifPresent(
												parentRawCommit ->
														queue.add(new RawCommitEntry(parentId, parentRawCommit))))))
				.thenRun(() ->
						doExcludeParents(
								queue, minLevel,
								resultHeads,
								cb))
				.whenException(cb::setException);
	}

	private static final class ServersAndHeads {
		final List<RawServer> servers;
		final Map<CommitId, SignedData<RawCommitHead>> heads;

		private ServersAndHeads(List<RawServer> servers, Map<CommitId, SignedData<RawCommitHead>> heads) {
			this.servers = servers;
			this.heads = heads;
		}
	}

	public Stage<Void> fetch() {
		return ensureServers()
				.thenCompose(servers -> Stages.firstSuccessful(servers.stream().map(this::fetch)));
	}

	public Stage<Void> fetch(RawServer server) {
		return extractHeadInfo()
				.thenCompose(headsInfo -> server.downloadStream(repositoryId, headsInfo.bases, headsInfo.heads)
						.streamTo(StreamConsumerWithResult.ofStage(getStreamConsumer()))
						.getConsumerResult());
	}

	public Stage<HeadsInfo> extractHeadInfo() {
		HeadsInfo headsInfo = new HeadsInfo(new HashSet<>(), new HashSet<>());
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenCompose(thisHeads -> Stages.all(
						thisHeads.stream()
								.map(head -> commitStorage.loadCommit(head)
										.whenResult(maybeRawCommit -> maybeRawCommit.ifPresent(
												rawCommit -> {
													headsInfo.heads.add(head);
													queue.add(new RawCommitEntry(head, rawCommit));
												}
										))))
						.thenCallback((Object $, Callback<Set<CommitId>> cb) ->
								extractHeadInfoImpl(queue, new HashSet<CommitId>(), cb))
						.whenResult(headsInfo.bases::addAll)
						.thenApply($ -> headsInfo));
	}

	private void extractHeadInfoImpl(PriorityQueue<RawCommitEntry> queue, Set<CommitId> bases, Callback<Set<CommitId>> cb) {
		RawCommitEntry entry = queue.poll();
		if (entry == null) {
			cb.set(bases);
			return;
		}
		Stages.all(
				entry.rawCommit.getParents().stream()
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
				.thenRun(() -> extractHeadInfoImpl(queue, bases, cb))
				.whenException(cb::setException);
	}

	private Stage<Void> push(RawServer server) {
		return server.getHeadsInfo(repositoryId)
				.thenCompose(headsInfo -> StreamProducer.ofStage(
						getStreamProducer(headsInfo.bases, headsInfo.heads))
						.streamTo(server.uploadStream(repositoryId))
						.getAcknowledgement());
	}

	private Set<CommitId> rawHeadsToCommitIds(Set<SignedData<RawCommitHead>> rawHeads) {
		return rawHeads.stream().map(rawHead -> rawHead.getData().commitId).collect(toSet());
	}

	public Stage<StreamConsumerWithResult<CommitEntry, Void>> getStreamConsumer() {
		return commitStorage.getHeads(repositoryId)
				.thenApply(Map::keySet)
				.thenApply(thisHeads -> {
					Set<CommitId> excludedHeads = new HashSet<>();
					Set<SignedData<RawCommitHead>> addedHeads = new HashSet<>();
					return StreamConsumerWithResult.ofSerialConsumer(SerialConsumer.of(
							(CommitEntry entry) -> {
								for (CommitId parentId : entry.getRawCommit().getParents()) {
									if (thisHeads.contains(parentId)) {
										excludedHeads.add(parentId);
									}
								}
								if (entry.hasRawHead()) {
									addedHeads.add(entry.getRawHead());
								}
								return commitStorage.saveCommit(entry.commitId, entry.rawCommit).toVoid();
							}))
							.thenCompose($ ->
									commitStorage.markCompleteCommits()
											.thenCompose($2 ->
													this.updateHeads(new HeadsDelta(addedHeads, excludedHeads))));
				});
	}

	public Stage<StreamProducer<CommitEntry>> getStreamProducer(Set<CommitId> thatBases, Set<CommitId> thatHeads) {
		checkArgument(!hasIntersection(thatBases, thatHeads));
		Set<CommitId> skipCommits = new HashSet<>(thatHeads);
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Stages.all(
						union(thisHeads.keySet(), thatBases, thatHeads).stream()
								.map(commitId -> commitStorage.loadCommit(commitId)
										.whenResult(maybeRawCommit -> maybeRawCommit.ifPresent(rawCommit ->
												queue.add(new RawCommitEntry(commitId, rawCommit))))
								))
						.thenApply($ -> AsyncSupplier.of(() -> getNextStreamEntry(queue, skipCommits, thatBases, thatHeads)))
						.thenApply(supplier -> supplier.transform(
								entry -> new CommitEntry(entry.commitId, entry.rawCommit, thisHeads.get(entry.commitId))))
						.thenApply(supplier -> AsyncSuppliers.prefetch(DOWNLOAD_PREFETCH_COUNT, supplier))
						.thenApply(supplier -> StreamProducer.ofSerialSupplier(SerialSupplier.of(supplier))));
	}

	private Stage<RawCommitEntry> getNextStreamEntry(PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> thatBases, Set<CommitId> thatHeads) {
		return Stage.ofCallback(cb -> getNextStreamEntry(queue, skipCommits, thatBases, thatHeads, cb));
	}

	private void getNextStreamEntry(PriorityQueue<RawCommitEntry> queue, Set<CommitId> skipCommits,
			Set<CommitId> thatBases, Set<CommitId> thatHeads,
			Callback<RawCommitEntry> cb) {
		if (queue.isEmpty() || queue.stream().map(RawCommitEntry::getCommitId).allMatch(skipCommits::contains)) {
			cb.set(null);
			return;
		}
		RawCommitEntry entry = queue.poll();
		boolean skipped = skipCommits.remove(entry.commitId);
		Stage.of(entry.getRawCommit().getParents())
				.thenCompose(nextCommitIds -> Stages.all(
						nextCommitIds.stream()
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
						}))
				.whenException(cb::setException);
	}

	public Stage<Void> save(Map<CommitId, RawCommit> newCommits, Set<SignedData<RawCommitHead>> newHeads) {
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Stages.all(
						newCommits.entrySet().stream()
								.map(entry -> commitStorage.saveCommit(entry.getKey(), entry.getValue())))
						.thenCompose($ -> {
							Set<CommitId> excludedHeads = new HashSet<>();
							for (RawCommit rawCommit : newCommits.values()) {
								excludedHeads.addAll(intersection(thisHeads.keySet(), rawCommit.getParents()));
							}
							return updateHeads(new HeadsDelta(newHeads, excludedHeads));
						}));
	}

	private Stage<Void> updateHeads(HeadsDelta headsDelta) {
		return commitStorage.updateHeads(repositoryId, headsDelta)
				.whenResult($ -> {
					if (!headsDelta.newHeads.isEmpty()) filterHeads.get();
				});
	}

/*
	public Stage<StreamProducer<CommitEntry>> streamProducer2(Set<CommitId> thatBases, Set<CommitId> thatHeads) {
		PriorityQueue<RawCommitEntry> queue = new PriorityQueue<>(reverseOrder());
		return commitStorage.getHeads(repositoryId)
				.thenCompose(thisHeads -> Stages.all(
						union(thisHeads.keySet(), thatBases).stream()
								.map(commitId -> commitStorage.loadCommit(commitId)
										.whenResult(maybeRawCommit -> maybeRawCommit.ifPresent(rawCommit ->
												queue.add(new RawCommitEntry(commitId, rawCommit))))
								))
						.thenApply($ -> AsyncSupplier.of(() -> getNextStreamEntry2(queue, thatHeads)))
						.thenApply(supplier -> supplier.map(
								entry -> new CommitEntry(entry.commitId, entry.rawCommit, thisHeads.get(entry.commitId))))
						.thenApply(supplier -> AsyncSuppliers.prefetch(DOWNLOAD_PREFETCH_COUNT, supplier))
						.thenApply(StreamProducer::ofAsyncSupplier));
	}

	private Stage<RawCommitEntry> getNextStreamEntry2(PriorityQueue<RawCommitEntry> queue, Set<CommitId> thatHeads) {
		return Stage.ofCallback(cb -> getNextStreamEntry2(queue, thatHeads, cb));
	}

	private void getNextStreamEntry2(PriorityQueue<RawCommitEntry> queue, Set<CommitId> thatHeads, Callback<RawCommitEntry> cb) {
		RawCommitEntry entry = queue.poll();
		if (entry == null) {
			cb.set(null);
			return;
		}
		Stage.of(entry.rawCommit.getParents())
				.thenCompose(nextCommitIds -> Stages.all(
						nextCommitIds.stream()
								.filter(nextCommitId -> !thatHeads.contains(nextCommitId))
								.filter(nextCommitId -> queue.stream().map(RawCommitEntry::getCommitId).noneMatch(nextCommitId::equals))
								.map(nextCommitId -> commitStorage.loadCommit(nextCommitId)
										.whenResult(maybeNextRawCommit -> maybeNextRawCommit.ifPresent(nextRawCommit ->
												queue.add(new RawCommitEntry(nextCommitId, nextRawCommit)))))))
				.thenRun(() -> cb.set(entry))
				.whenException(cb::setException);
	}
*/

	public RepositoryName getRepositoryId() {
		return repositoryId;
	}

}
