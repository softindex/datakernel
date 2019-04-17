package io.global.chat.service;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;
import io.global.ot.client.OTDriver;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

public final class RoomListServiceHolder implements EventloopService {
	private final Eventloop eventloop;
	private final OTDriver driver;
	private final Map<PrivKey, RoomListService> services = new HashMap<>();

	private RoomListServiceHolder(Eventloop eventloop, OTDriver driver) {
		this.eventloop = eventloop;
		this.driver = driver;
	}

	public static RoomListServiceHolder create(Eventloop eventloop, OTDriver driver) {
		return new RoomListServiceHolder(eventloop, driver);
	}

	public void ensureRoomListService(PrivKey privKey) {
		if (!services.containsKey(privKey)) {
			RoomListService service = RoomListService.create(eventloop, driver, privKey);
			services.put(privKey, service);
			service.start()
					.whenException(e -> services.remove(privKey));
		}
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
		return Promises.all(services.values().stream().map(RoomListService::stop)).materialize();
	}
}
