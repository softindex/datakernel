package io.global.ot.service;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTSystem;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.GlobalPmDriver;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

import static io.global.ot.shared.SharedReposOTSystem.createOTSystem;

public final class UserContainerHolder<D> implements EventloopService {
	public static final OTSystem<SharedReposOperation> LIST_SYSTEM = createOTSystem();

	private final Eventloop eventloop;
	private final OTDriver driver;
	private final OTSystem<D> otSystem;
	private final StructuredCodec<D> diffCodec;
	private final GlobalPmDriver<CreateSharedRepo> pmDriver;
	private final String indexRepoName;
	private final String sharedRepoPrefix;
	private final Map<PubKey, MaterializedPromise<UserContainer<D>>> containers = new HashMap<>();

	private UserContainerHolder(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec,
			GlobalPmDriver<CreateSharedRepo> pmDriver, String indexRepoName, String sharedRepoPrefix) {
		this.eventloop = eventloop;
		this.driver = driver;
		this.otSystem = otSystem;
		this.pmDriver = pmDriver;
		this.diffCodec = diffCodec;
		this.indexRepoName = indexRepoName;
		this.sharedRepoPrefix = sharedRepoPrefix;
	}

	public static <D> UserContainerHolder<D> create(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec,
			GlobalPmDriver<CreateSharedRepo> pmDriver, String indexRepoName, String sharedRepoPrefix) {
		return new UserContainerHolder<>(eventloop, driver, otSystem, diffCodec, pmDriver, indexRepoName, sharedRepoPrefix);
	}

	public Promise<UserContainer<D>> ensureUserContainer(PrivKey privKey) {
		PubKey pubKey = privKey.computePubKey();
		if (!containers.containsKey(pubKey)) {
			UserContainer<D> container = getUserContainer(privKey);
			MaterializedPromise<UserContainer<D>> containerPromise = container.start()
					.map($ -> container)
					.materialize();
			containers.put(pubKey, containerPromise);
			containerPromise
					.whenException(e -> containers.remove(pubKey));
			return containerPromise;
		} else {
			return containers.get(pubKey);
		}
	}

	private UserContainer<D> getUserContainer(PrivKey privKey) {
		RepoID repoID = RepoID.of(privKey, sharedRepoPrefix);
		MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(repoID, privKey, diffCodec);
		return UserContainer.create(eventloop, driver, otSystem, myRepositoryId, pmDriver, indexRepoName);
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
		return Promises.all(containers.values().stream()
				.map(containerPromise -> containerPromise.getResult().stop()))
				.materialize();
	}
}
