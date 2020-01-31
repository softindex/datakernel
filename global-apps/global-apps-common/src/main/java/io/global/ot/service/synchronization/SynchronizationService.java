package io.global.ot.service.synchronization;

import io.datakernel.async.service.EventloopService;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.ot.api.RepoID;
import io.global.ot.client.OTDriver;
import io.global.ot.client.RepoSynchronizer;
import io.global.ot.service.messaging.MessagingService;
import io.global.ot.shared.SharedRepo;
import io.global.ot.shared.SharedReposOTState;
import io.global.ot.shared.SharedReposOperation;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.promise.Promises.retry;

public final class SynchronizationService<D> implements EventloopService {
	@Inject
	private Eventloop eventloop;
	@Inject
	private OTDriver driver;
	@Inject
	private OTSystem<D> system;
	@Inject
	@Named("initial backoff")
	private Duration initialBackoff;
	@Inject
	private Function<String, RepoSynchronizer<D>> repoSynchronizerFactory;
	@Inject
	private MessagingService messagingService;
	@Inject
	private KeyPair keys;
	@Inject
	@Named("repo prefix")
	private String repoPrefix;

	private SharedReposOTState resourceListState;
	private final Map<String, RepoSynchronizer<D>> synchronizers = new HashMap<>();

	private SynchronizationService(SharedReposOTState sharedReposOTState) {
		this.resourceListState = sharedReposOTState;
	}

	@Inject
	public static <D> SynchronizationService<D> create(OTState<SharedReposOperation> state) {
		return new SynchronizationService<>((SharedReposOTState) state);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		Set<SharedRepo> sharedRepos = resourceListState.getSharedRepos();
		sharedRepos.forEach(resource -> {
			ensureSynchronizer(resource.getId()).sync(resource.getParticipants());
			sendMessageToParticipants(resource);
		});

		resourceListState.setListener(sharedReposOperation -> {
			SharedRepo sharedRepo = sharedReposOperation.getSharedRepo();
			if (sharedReposOperation.isRemove()) {
				synchronizers.remove(sharedRepo.getId()).stop();
			} else {
				ensureSynchronizer(sharedRepo.getId()).sync(sharedRepo.getParticipants());
				sendMessageToParticipants(sharedRepo);
			}
		});

		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promises.all(synchronizers.values().stream().map(RepoSynchronizer::stop));
	}

	public RepoSynchronizer<D> ensureSynchronizer(String id) {
		return synchronizers.computeIfAbsent(id, repoSynchronizerFactory);
	}

	public void sendMessageToParticipants(SharedRepo sharedRepo) {
		Set<PubKey> participants = sharedRepo.getParticipants();
		String id = sharedRepo.getId();
		Promises.all(participants.stream()
				.filter(participant -> !sharedRepo.isMessageSent(participant) &&
						!participant.equals(keys.getPubKey()))
				.map(participant ->
						driver.getHeads(RepoID.of(participant, repoPrefix + id))
								.whenComplete((heads, e) -> {
									if (e == null && !heads.isEmpty() && !first(heads).isRoot()) {
										sharedRepo.setMessageSent(participant);
									} else {
										retry(() -> messagingService
												.sendCreateMessage(participant, id, sharedRepo.getName(), participants))
												.get()
												.whenResult($ -> sharedRepo.setMessageSent(participant));
									}
								})));
	}
}
