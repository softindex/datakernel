package io.global.ot.service.messaging;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.ot.OTStateManager;
import io.datakernel.util.ApplicationSettings;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.service.CommonUserContainer;
import io.global.ot.shared.CreateOrDropRepo;
import io.global.ot.shared.SharedRepo;
import io.global.ot.shared.SharedReposOTState;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.Message;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Set;

import static io.datakernel.async.AsyncSuppliers.retry;
import static io.datakernel.async.Promises.repeat;
import static io.global.ot.OTUtils.POLL_RETRY_POLICY;
import static io.global.util.Utils.eitherComplete;

public final class MessagingService implements EventloopService {
	public static final StacklessException STOPPED_EXCEPTION = new StacklessException(MessagingService.class, "Service has been stopped");
	@NotNull
	public static final Duration POLL_INTERVAL = ApplicationSettings.getDuration(MessagingService.class, "message.poll.interval", Duration.ofSeconds(5));

	private final Eventloop eventloop;
	private final GlobalPmDriver<CreateSharedRepo> pmDriver;
	private final CommonUserContainer<?> commonUserContainer;
	private final String mailBox;

	private SettablePromise<Message<CreateSharedRepo>> stopPromise = new SettablePromise<>();

	private MessagingService(Eventloop eventloop, GlobalPmDriver<CreateSharedRepo> pmDriver, CommonUserContainer<?> commonUserContainer, String mailBox) {
		this.eventloop = eventloop;
		this.pmDriver = pmDriver;
		this.commonUserContainer = commonUserContainer;
		this.mailBox = mailBox;
	}

	public static MessagingService create(Eventloop eventloop, GlobalPmDriver<CreateSharedRepo> pmDriver,
			CommonUserContainer<?> commonUserContainer, String mailBox) {
		return new MessagingService(eventloop, pmDriver, commonUserContainer, mailBox);
	}

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
		stopPromise.trySetException(STOPPED_EXCEPTION);
		return Promise.complete();
	}

	public Promise<Void> sendCreateMessage(PubKey receiver, String id, String name, Set<PubKey> participants) {
		MyRepositoryId<?> myRepositoryId = commonUserContainer.getMyRepositoryId();
		PrivKey senderPrivKey = myRepositoryId.getPrivKey();
		PubKey senderPubKey = senderPrivKey.computePubKey();
		CreateSharedRepo payload = new CreateSharedRepo(new SharedRepo(id, name, participants));
		Message<CreateSharedRepo> message = Message.now(senderPubKey, payload);
		return pmDriver.send(senderPrivKey, receiver, mailBox, message);
	}

	@SuppressWarnings("ConstantConditions")
	private void pollMessages() {
		KeyPair keys = commonUserContainer.getMyRepositoryId().getPrivKey().computeKeys();
		OTStateManager<CommitId, SharedReposOperation> stateManager = commonUserContainer.getStateManager();
		SharedReposOTState state = (SharedReposOTState) stateManager.getState();
		AsyncSupplier<@Nullable Message<CreateSharedRepo>> messagesSupplier = retry(() -> pmDriver.poll(keys, mailBox),
				POLL_RETRY_POLICY);

		repeat(() -> eitherComplete(messagesSupplier.get(), stopPromise)
				.then(message -> {
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
								.then($ -> pmDriver.drop(keys, mailBox, message.getId()))
								.toTry()
								.toVoid();
					}
					return Promises.delay(POLL_INTERVAL, Promise.complete());
				}));
	}

}
