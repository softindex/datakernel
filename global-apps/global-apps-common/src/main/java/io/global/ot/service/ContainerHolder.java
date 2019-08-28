package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;

public final class ContainerHolder<C extends UserContainer> implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(ContainerHolder.class);

	private final Eventloop eventloop;
	private final BiFunction<Eventloop, PrivKey, C> containerFactory;
	private final Map<PubKey, Promise<C>> containers = new HashMap<>();

	public ContainerHolder(Eventloop eventloop, BiFunction<Eventloop, PrivKey, C> containerFactory) {
		this.eventloop = eventloop;
		this.containerFactory = containerFactory;
	}

	public Promise<C> ensureUserContainer(PrivKey privKey) {
		PubKey pubKey = privKey.computePubKey();
		return containers.computeIfAbsent(pubKey,
				$1 -> {
					C container = containerFactory.apply(eventloop, privKey);
					return container.start()
							.map($2 -> container);
				})
				.whenException(e -> containers.remove(pubKey))
				.whenComplete(toLogger(logger, thisMethod(), pubKey));
	}

	public Promise<?> removeUserContainer(PubKey pubKey) {
		Promise<C> promise = containers.remove(pubKey);
		return promise == null ? Promise.complete() : promise.then(EventloopService::stop);
	}

	public Promise<@Nullable C> getUserContainer(PubKey pubKey) {
		Promise<C> containerPromise = containers.get(pubKey);
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
				.map(containerPromise -> containerPromise.getResult().stop()))
				.whenResult($ -> containers.clear());
	}

	public Set<PubKey> getContainerKeys() {
		return containers.keySet();
	}
}
