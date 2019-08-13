package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public final class ContainerHolder<T extends EventloopService> implements EventloopService {
	private final Eventloop eventloop;
	private final BiFunction<Eventloop, PrivKey, T> containerFactory;
	private final Map<PubKey, Promise<T>> containers = new HashMap<>();

	public ContainerHolder(Eventloop eventloop, BiFunction<Eventloop, PrivKey, T> containerFactory) {
		this.eventloop = eventloop;
		this.containerFactory = containerFactory;
	}

	public Promise<T> ensureUserContainer(PrivKey privKey) {
		PubKey pubKey = privKey.computePubKey();
		if (!containers.containsKey(pubKey)) {
			T container = containerFactory.apply(eventloop, privKey);
			Promise<T> containerPromise = container.start()
					.map($ -> container);
			containers.put(pubKey, containerPromise);
			containerPromise
					.whenException(e -> containers.remove(pubKey));
			return containerPromise;
		} else {
			return containers.get(pubKey);
		}
	}

	public Promise<@Nullable T> getUserContainer(PubKey pubKey) {
		Promise<T> containerPromise = containers.get(pubKey);
		return containerPromise == null ? Promise.of(null) : containerPromise;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promises.all(containers.values().stream()
				.map(containerPromise -> containerPromise.getResult().stop()));
	}
}
