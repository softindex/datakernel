package io.global.ot.service;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.*;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.service.messaging.MessagingService;
import io.global.ot.service.synchronization.SynchronizationService;
import io.global.ot.session.UserId;
import io.global.ot.shared.SharedReposOTState;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.Messenger;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;

import static io.global.ot.OTUtils.POLL_RETRY_POLICY;
import static io.global.ot.OTUtils.SHARED_REPOS_OPERATION_CODEC;
import static io.global.ot.shared.SharedReposOTSystem.createOTSystem;
import static java.util.Collections.emptySet;

public final class CommonUserContainer<D> implements UserContainer {
	private static final OTSystem<SharedReposOperation> LIST_SYSTEM = createOTSystem();
	private final Eventloop eventloop;
	private final MyRepositoryId<D> myRepositoryId;
	private final OTStateManager<CommitId, SharedReposOperation> stateManager;

	private final KvSessionStore<UserId> sessionStore;
	private final SynchronizationService<D> synchronizationService;
	private final MessagingService messagingService;

	private CommonUserContainer(Eventloop eventloop, MyRepositoryId<D> myRepositoryId, OTDriver driver, OTSystem<D> otSystem,
			OTStateManager<CommitId, SharedReposOperation> stateManager, Messenger<Long, CreateSharedRepo> messenger,
			KvSessionStore<UserId> sessionStore, String indexRepoName) {
		this.eventloop = eventloop;
		this.myRepositoryId = myRepositoryId;
		this.stateManager = stateManager;
		this.sessionStore = sessionStore;
		this.synchronizationService = SynchronizationService.create(eventloop, driver, this, otSystem);
		this.messagingService = MessagingService.create(eventloop, messenger, this, indexRepoName);
	}

	public static <D> CommonUserContainer<D> create(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem, MyRepositoryId<D> myRepositoryId,
			Messenger<Long, CreateSharedRepo> messenger, KvSessionStore<UserId> sessionStore, String indexRepoName) {
		OTStateManager<CommitId, SharedReposOperation> stateManager = createStateManager(eventloop, driver, myRepositoryId, indexRepoName);
		return new CommonUserContainer<>(eventloop, myRepositoryId, driver, otSystem, stateManager, messenger, sessionStore, indexRepoName);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete()
				.then($ -> stateManager.start())
				.then($ -> synchronizationService.start())
				.then($ -> messagingService.start());
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete()
				.then($ -> stateManager.stop())
				.then($ -> synchronizationService.stop())
				.then($ -> messagingService.stop());
	}

	@Override
	public KeyPair getKeys() {
		return myRepositoryId.getPrivKey().computeKeys();
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}

	public MyRepositoryId<D> getMyRepositoryId() {
		return myRepositoryId;
	}

	public SharedReposOTState getResourceListState() {
		return (SharedReposOTState) stateManager.getState();
	}

	public OTStateManager<CommitId, SharedReposOperation> getStateManager() {
		return stateManager;
	}

	public MessagingService getMessagingService() {
		return messagingService;
	}

	private static <D> OTStateManager<CommitId, SharedReposOperation> createStateManager(Eventloop eventloop, OTDriver driver,
			MyRepositoryId<D> myRepositoryId, String resourceListRepoName) {
		RepoID repoID = RepoID.of(myRepositoryId.getPrivKey(), resourceListRepoName);
		MyRepositoryId<SharedReposOperation> listRepositoryId = new MyRepositoryId<>(repoID, myRepositoryId.getPrivKey(), SHARED_REPOS_OPERATION_CODEC);
		OTRepositoryAdapter<SharedReposOperation> repository = new OTRepositoryAdapter<>(driver, listRepositoryId, emptySet());
		OTState<SharedReposOperation> state = new SharedReposOTState();
		OTUplink<CommitId, SharedReposOperation, OTCommit<CommitId, SharedReposOperation>> uplink = OTUplinkImpl.create(repository, LIST_SYSTEM);
		return OTStateManager.create(eventloop, LIST_SYSTEM, uplink, state)
				.withPoll(POLL_RETRY_POLICY);
	}
}
