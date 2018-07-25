package io.global.globalsync.server;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.*;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.time.CurrentTimeProvider;
import io.global.globalsync.api.*;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class RawServerImpl implements RawServer, EventloopService {
	private final Eventloop eventloop;
	private RawServerId myServerId;
	private final RawDiscoveryService discoveryService;
	private final CommitStorage commitStorage;

	public interface Settings {
		RawServer_PubKey.Settings getPubKeySettings(PubKey pubKey);
	}

	private final Settings settings;
	private final Map<PubKey, RawServer_PubKey> pubKeyDbMap = new HashMap<>();
	private final Map<PubKey, SharedKeysDb> sharedKeysDb = new HashMap<>();

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();
	RawServerFactory rawServerFactory;

	private Set<CommitId> parents = new HashSet<>();

	public RawServerImpl(Eventloop eventloop, RawDiscoveryService discoveryService, CommitStorage commitStorage, Settings settings) {
		this.eventloop = eventloop;
		this.discoveryService = discoveryService;
		this.commitStorage = commitStorage;
		this.settings = settings;
	}

	private RawServer_Repository ensureRepositoryDb(RepositoryName repositoryId) {
		RawServer_PubKey pubKeyDb = pubKeyDbMap.computeIfAbsent(repositoryId.getPubKey(),
				k -> new RawServer_PubKey(discoveryService, commitStorage, k, settings.getPubKeySettings(k)));
		return pubKeyDb.ensureName(repositoryId);
	}

	private SharedKeysDb ensureSharedKeysDb(PubKey receiver) {
		return sharedKeysDb.computeIfAbsent(receiver, SharedKeysDb::new);
	}

	@Override
	public Stage<Set<String>> getRepositories(PubKey pubKey) {
		return Stage.of(new HashSet<>(pubKeyDbMap.computeIfAbsent(pubKey, k -> new RawServer_PubKey(discoveryService, commitStorage, k, settings.getPubKeySettings(k)))
				.repositories.values().stream()
				.map(RawServer_Repository::getRepositoryId)
				.map(RepositoryName::getRepositoryName)
				.collect(toSet())));
	}

	@Override
	public Stage<Void> save(RepositoryName repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
		return ensureRepositoryDb(repositoryId).save(commits, heads);
	}

	@Override
	public Stage<RawCommit> loadCommit(RepositoryName repositoryId, CommitId id) {
		return commitStorage.loadCommit(id)
				.thenApply(Optional::get);
	}

	@Override
	public Stage<StreamProducer<CommitEntry>> download(RepositoryName repositoryId, Set<CommitId> bases, Set<CommitId> heads) {
		return ensureRepositoryDb(repositoryId)
				.getStreamProducer(bases, heads);
	}

	@Override
	public Stage<HeadsInfo> getHeadsInfo(RepositoryName repositoryId) {
		return ensureRepositoryDb(repositoryId).extractHeadInfo();
	}

	@Override
	public Stage<StreamConsumer<CommitEntry>> upload(RepositoryName repositoryId) {
		return ensureRepositoryDb(repositoryId)
				.getStreamConsumer()
				.thenApply(identity());
	}

	@Override
	public Stage<Void> saveSnapshot(RepositoryName repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		CommitId commitId = encryptedSnapshot.getData().commitId;
		RawServer_Repository repositoryDb = ensureRepositoryDb(repositoryId);
		return commitStorage.saveSnapshot(encryptedSnapshot)
				.thenCompose(saved -> saved ?
						repositoryDb.ensureServers()
								.thenCompose(servers -> Stages.any(servers.stream()
										.map(server -> server.saveSnapshot(repositoryId, encryptedSnapshot)))) :
						Stage.of(null));
	}

	@Override
	public Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId commitId) {
		RawServer_Repository repositoryDb = ensureRepositoryDb(repositoryId);
		return Stages.firstSuccessful(
				() -> commitStorage.loadSnapshot(repositoryId, commitId)
						.thenTry(Optional::get),
				() -> repositoryDb.ensureServers()
						.thenCompose(servers -> Stages.firstSuccessful(
								servers.stream()
										.map(server -> server.loadSnapshot(repositoryId, commitId)
												.thenTry(Optional::get)))))
				.thenApplyEx((maybeResult, e) -> e == null ? Optional.of(maybeResult) : Optional.empty());
	}

	@Override
	public Stage<HeadsDelta> getHeads(RepositoryName repositoryId, Set<CommitId> remoteHeads) {
		RawServer_Repository repositoryDb = ensureRepositoryDb(repositoryId);
//		if (repositoryDb.isSync() || repositoryDb.isUpdate()) {
//			return Stage.of(repositoryDb.heads);
//		}
		return repositoryDb.getHeads(remoteHeads);
	}

	@Override
	public Stage<Void> shareKey(SignedData<SharedSimKey> simKey) {
		ensureSharedKeysDb(simKey.getData().getReceiver())
				.sharedKeysBySender
				.computeIfAbsent(simKey.getData().getRepositoryOwner(), $ -> new HashMap<>())
				.put(simKey.getData().getSimKeyHash(), simKey);
		return Stage.of(null);
	}

	@Override
	public Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, SimKeyHash simKeyHash) {
		return Stage.of(Optional.ofNullable(
				ensureSharedKeysDb(receiver)
						.sharedKeysBySender
						.getOrDefault(owner, emptyMap())
						.get(simKeyHash)));
	}

	@Override
	public Stage<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		return commitStorage.savePullRequest(pullRequest)
				.thenCompose(saveStatus -> saveStatus ?
						ensureRepositoryDb(pullRequest.getData().repository).ensureServers()
								.thenCompose(servers -> Stages.any(
										servers.stream()
												.map(server -> server.sendPullRequest(pullRequest))
								)) :
						Stage.of(null));
	}

	@Override
	public Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepositoryName repositoryId) {
		return ensureRepositoryDb(repositoryId).getPullRequests();
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
		return Stage.of(null);
	}

	private Stage<RawCommit> loadAndCacheCommit(RepositoryName repositoryId, List<? extends RawServer> servers, CommitId commitId) {
		return commitStorage.loadCommit(commitId)
				.thenCompose(maybeRawCommit -> {
					if (maybeRawCommit.isPresent()) return Stage.of(maybeRawCommit.get());
					return Stages.firstSuccessful(servers.stream()
							.map(server -> server.loadCommit(repositoryId, commitId)))
							.thenCompose(rawCommit -> commitStorage.saveCommit(commitId, rawCommit)
									.thenApply($ -> rawCommit));
				});
	}

	static final class SharedKeysDb {
		public final PubKey receiver;
		public final Map<PubKey, Map<SimKeyHash, SignedData<SharedSimKey>>> sharedKeysBySender = new HashMap<>();

		SharedKeysDb(PubKey receiver) {
			this.receiver = receiver;
		}
	}

}
