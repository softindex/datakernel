package io.global.ot.service.synchronization;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTSystem;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.RepoSynchronizer;
import io.global.ot.service.UserContainer;
import io.global.ot.shared.SharedRepo;
import io.global.ot.shared.SharedReposOTState;
import io.global.ot.shared.SharedReposOperation;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class SynchronizationService<D> implements EventloopService {
	private final Eventloop eventloop;
	private final UserContainer<D> userContainer;
	private final OTDriver driver;
	private final OTSystem<D> system;
	private final Map<String, RepoSynchronizer<D>> synchronizers = new HashMap<>();

	private SynchronizationService(Eventloop eventloop, UserContainer<D> userContainer, OTDriver driver, OTSystem<D> system) {
		this.eventloop = eventloop;
		this.userContainer = userContainer;
		this.driver = driver;
		this.system = system;
	}

	public static <D> SynchronizationService<D> create(Eventloop eventloop, OTDriver driver, UserContainer<D> userContainer, OTSystem<D> system) {
		return new SynchronizationService<>(eventloop, userContainer, driver, system);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		SharedReposOTState resourceListState = userContainer.getResourceListState();
		resourceListState.getSharedRepos()
				.forEach(resource -> ensureSynchronizer(resource.getId()).sync(resource.getParticipants()));
		resourceListState.setListener(new ResourceListListener());
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promises.all(synchronizers.values().stream().map(RepoSynchronizer::stop))
				.materialize();

	}

	public RepoSynchronizer<D> ensureSynchronizer(String id) {
		return synchronizers.computeIfAbsent(id, $ ->
				RepoSynchronizer.create(eventloop, driver, system, getMyRepositoryId(id)));
	}

	private MyRepositoryId<D> getMyRepositoryId(String id) {
		MyRepositoryId<D> myRepositoryId = userContainer.getMyRepositoryId();
		String name = myRepositoryId.getRepositoryId().getName() + '/' + id;
		RepoID repoId = RepoID.of(myRepositoryId.getPrivKey(), name);
		return new MyRepositoryId<>(repoId, myRepositoryId.getPrivKey(), myRepositoryId.getDiffCodec());
	}

	final class ResourceListListener implements Consumer<SharedReposOperation> {
		@Override
		public void accept(SharedReposOperation sharedReposOperation) {
			SharedRepo sharedRepo = sharedReposOperation.getSharedRepo();
			if (sharedReposOperation.isRemove()) {
				synchronizers.remove(sharedRepo.getId()).stop();
			} else {
				ensureSynchronizer(sharedRepo.getId()).sync(sharedRepo.getParticipants());
			}
		}
	}
}
