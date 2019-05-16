package io.global.fs.app.container;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.fs.app.container.message.SharedDirMessage;
import io.global.fs.local.GlobalFsDriver;
import io.global.pm.GlobalPmDriver;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

public final class FsUserContainerHolder implements EventloopService {
	private final Eventloop eventloop;
	private final GlobalFsDriver fsDriver;
	private final GlobalPmDriver<SharedDirMessage> pmDriver;

	private final Map<PubKey, MaterializedPromise<FsUserContainer>> containers = new HashMap<>();

	private FsUserContainerHolder(Eventloop eventloop, GlobalFsDriver fsDriver, GlobalPmDriver<SharedDirMessage> pmDriver) {
		this.eventloop = eventloop;
		this.fsDriver = fsDriver;
		this.pmDriver = pmDriver;
	}

	public static FsUserContainerHolder create(Eventloop eventloop, GlobalFsDriver fsDriver, GlobalPmDriver<SharedDirMessage> pmDriver) {
		return new FsUserContainerHolder(eventloop, fsDriver, pmDriver);
	}

	public Promise<FsUserContainer> ensureUserContainer(PrivKey privKey) {
		PubKey pubKey = privKey.computePubKey();
		if (!containers.containsKey(pubKey)) {
			FsUserContainer container = FsUserContainer.create(eventloop, privKey, fsDriver, pmDriver);
			MaterializedPromise<FsUserContainer> containerPromise = container.start()
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

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<?> start() {
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
