package io.global.fs.app.container;

import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.util.ApplicationSettings;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SimKey;
import io.global.fs.app.container.message.CreateSharedDirMessage;
import io.global.fs.app.container.message.SharedDirMessage;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.Message;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Set;

import static io.datakernel.async.AsyncSuppliers.retry;
import static io.datakernel.async.Promises.repeat;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;
import static io.global.fs.app.container.Utils.RETRY_POLICY;
import static io.global.util.Utils.eitherComplete;

public final class FsMessagingService implements EventloopService {
	public static final int SHARED_DIR_ID_LENGTH = ApplicationSettings.getInt(FsMessagingService.class, "dir.id.length", 32);
	public static final StacklessException STOPPED_EXCEPTION = new StacklessException(FsMessagingService.class, "Service has been stopped");
	public static final Duration POLL_INTERVAL = ApplicationSettings.getDuration(FsMessagingService.class, "message.poll.interval", Duration.ofSeconds(5));

	private final Eventloop eventloop;
	private final GlobalPmDriver<SharedDirMessage> driver;
	private final String mailBox;
	private final KeyPair keys;
	private final FsSyncService syncService;
	private final SettablePromise<Message<SharedDirMessage>> stopPromise = new SettablePromise<>();

	private FsMessagingService(String mailBox, FsUserContainer userContainer) {
		this.eventloop = userContainer.getEventloop();
		this.driver = userContainer.getPmDriver();
		this.mailBox = mailBox;
		this.keys = userContainer.getKeys();
		this.syncService = userContainer.getSyncService();
	}

	public static FsMessagingService create(String mailBox, FsUserContainer userContainer) {
		return new FsMessagingService(mailBox, userContainer);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		pollMessages();
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		stopPromise.trySetException(STOPPED_EXCEPTION);
		return Promise.complete();
	}

	public Promise<Void> sendMessage(String dirName, Set<PubKey> participants) {
		String dirId = generateRandomHexString();
		SimKey dirSimKey = SimKey.generate();
		participants.add(keys.getPubKey());
		return Promises.all(participants.stream()
				.map(receiver -> {
					SharedSimKey sharedSimKey = SharedSimKey.of(dirSimKey, receiver);
					SharedDirMetadata dirMetadata = new SharedDirMetadata(dirId, dirName, participants, sharedSimKey);
					CreateSharedDirMessage payload = new CreateSharedDirMessage(dirMetadata);
					Message<SharedDirMessage> message = Message.now(keys.getPubKey(), payload);
					return driver.send(keys.getPrivKey(), receiver, mailBox, message);
				}));
	}

	@SuppressWarnings("ConstantConditions")
	private void pollMessages() {
		AsyncSupplier<@Nullable Message<SharedDirMessage>> messagesSupplier = retry(() -> driver.poll(keys, mailBox), RETRY_POLICY);

		repeat(() -> eitherComplete(messagesSupplier.get(), stopPromise)
				.then(message -> {
					if (message == null) {
						return Promises.delay(Promise.complete(), POLL_INTERVAL);
					} else {
						SharedDirMessage payload = message.getPayload();
						return syncService.ensureSynchronizer(payload.getDirMetadata())
								.then($ -> driver.drop(keys, mailBox, message.getId()))
								.toTry()
								.toVoid();
					}
				}));
	}

	private static String generateRandomHexString() {
		return toHexString(randomBytes(SHARED_DIR_ID_LENGTH));
	}

}
