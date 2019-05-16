package io.global.fs.app.container;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.fs.app.container.message.SharedDirMessage;
import io.global.fs.local.GlobalFsDriver;
import io.global.pm.GlobalPmDriver;
import org.jetbrains.annotations.NotNull;

import static io.global.fs.app.container.Utils.FS_SHARED_DIR_MAILBOX;

public final class FsUserContainer implements EventloopService {
	private final Eventloop eventloop;
	private final KeyPair keys;
	private final GlobalFsDriver fsDriver;
	private final GlobalPmDriver<SharedDirMessage> pmDriver;

	private final FsSyncService syncService;
	private final FsMessagingService messagingService;

	private FsUserContainer(Eventloop eventloop, PrivKey privateKey, GlobalFsDriver fsDriver, GlobalPmDriver<SharedDirMessage> pmDriver) {
		this.eventloop = eventloop;
		this.keys = privateKey.computeKeys();
		this.fsDriver = fsDriver;
		this.pmDriver = pmDriver;
		this.syncService = FsSyncService.create(this);
		this.messagingService = FsMessagingService.create(FS_SHARED_DIR_MAILBOX, this);
	}

	public static FsUserContainer create(Eventloop eventloop, PrivKey privateKey, GlobalFsDriver fsDriver, GlobalPmDriver<SharedDirMessage> pmDriver) {
		return new FsUserContainer(eventloop, privateKey, fsDriver, pmDriver);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return Promise.complete()
				.then($ -> syncService.start())
				.then($ -> messagingService.start())
				.materialize();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promise.complete()
				.then($ -> syncService.stop())
				.then($ -> messagingService.stop())
				.materialize();
	}

	public KeyPair getKeys() {
		return keys;
	}

	public GlobalFsDriver getFsDriver() {
		return fsDriver;
	}

	public GlobalPmDriver<SharedDirMessage> getPmDriver() {
		return pmDriver;
	}

	public FsSyncService getSyncService() {
		return syncService;
	}

	public FsMessagingService getMessagingService() {
		return messagingService;
	}
}
