package io.global.ot.service.messaging;

import io.datakernel.async.service.EventloopService;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.ot.api.CommitId;
import io.global.ot.shared.CreateOrDropRepo;
import io.global.ot.shared.SharedRepo;
import io.global.ot.shared.SharedReposOTState;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Set;

@Inject
public final class MessagingService implements EventloopService {
	public static final StacklessException STOPPED_EXCEPTION = new StacklessException(MessagingService.class, "Service has been stopped");
	public static final Duration DEFAULT_POLL_INTERVAL = ApplicationSettings.getDuration(MessagingService.class, "message.poll.interval", Duration.ofSeconds(5));

	@Inject
	private Eventloop eventloop;
	@Inject
	private PmClient<CreateSharedRepo> pmClient;
	@Inject
	private KeyPair keys;
	@Inject
	@Named("mail box")
	private String mailBox;

	@Inject
	@Named("poll interval")
	private Duration pollInterval;

	@Inject
	OTStateManager<CommitId, SharedReposOperation> stateManager;

	private ChannelSupplier<Message<CreateSharedRepo>> messageSupplier;

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		pollMessages();
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		messageSupplier.close(STOPPED_EXCEPTION);
		return Promise.complete();
	}

	public Promise<Void> sendCreateMessage(PubKey receiver, String id, String name, Set<PubKey> participants) {
		CreateSharedRepo payload = new CreateSharedRepo(new SharedRepo(id, name, participants));
		return pmClient.send(receiver, mailBox, payload);
	}

	private void pollMessages() {
		SharedReposOTState state = (SharedReposOTState) stateManager.getState();
		messageSupplier = ChannelSupplier.ofPromise(pmClient.stream(mailBox));
		messageSupplier.streamTo(ChannelConsumer.of(message -> {
			if (message != null) {
				CreateSharedRepo createSharedRepo = message.getPayload();
				SharedRepo sharedRepo = createSharedRepo.getSharedRepo();
				return Promise.complete()
						.then($ -> {
							if (!state.getSharedRepos().contains(sharedRepo)) {
								CreateOrDropRepo createOp = CreateOrDropRepo.create(sharedRepo);
								stateManager.add(createOp);
								return stateManager.sync()
										.whenException(e -> stateManager.reset());
							} else {
								return Promise.complete();
							}
						})
						.then($ -> pmClient.drop(mailBox, message.getId()))
						.toTry()
						.toVoid();
			}
			return Promises.delay(pollInterval, Promise.complete());
		}));
	}

}
