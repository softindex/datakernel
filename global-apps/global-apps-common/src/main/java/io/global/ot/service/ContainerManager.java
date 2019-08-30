package io.global.ot.service;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.EventloopTaskScheduler.Schedule;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.util.Tuple2;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;

public final class ContainerManager<C extends UserContainer> implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(ContainerManager.class);

	private static final Schedule DEFAULT_SYNC_SCHEDULE = Schedule.ofInterval(Duration.ofSeconds(5));

	private final Eventloop eventloop;
	private final BiFunction<Eventloop, PrivKey, C> containerFactory;

	private final Map<PubKey, Promise<C>> pendingContainers = new HashMap<>();
	private final Map<PubKey, Tuple2<PrivKey, C>> containers = new HashMap<>();

	private final EventloopTaskScheduler synchronizer;
	private final ContainerKeyManager keyManager;

	private ContainerManager(Eventloop eventloop, ContainerKeyManager keyManager, BiFunction<Eventloop, PrivKey, C> containerFactory) {
		this.eventloop = eventloop;
		this.containerFactory = containerFactory;
		this.keyManager = keyManager;
		this.synchronizer = EventloopTaskScheduler.create(eventloop, this::sync)
				.withSchedule(DEFAULT_SYNC_SCHEDULE);
	}

	public static <C extends UserContainer> ContainerManager<C> create(Eventloop eventloop, ContainerKeyManager keyManager, BiFunction<Eventloop, PrivKey, C> containerFactory) {
		return new ContainerManager<>(eventloop, keyManager, containerFactory);
	}

	public ContainerManager<C> withSyncSchedule(Schedule schedule) {
		this.synchronizer.setSchedule(schedule);
		return this;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return sync().then($ -> synchronizer.start());
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return synchronizer.stop()
				.then($ -> Promises.all(containers.values().stream()
						.map(tuple -> tuple.getValue2().stop()))
						.whenResult($2 -> containers.clear())
				);
	}

	private Promise<C> ensureUserContainer(PrivKey privKey) {
		PubKey pubKey = privKey.computePubKey();
		Tuple2<PrivKey, C> tuple = containers.get(pubKey);
		if (tuple != null) {
			return Promise.of(tuple.getValue2());
		}
		return pendingContainers.computeIfAbsent(pubKey,
				$ -> {
					C container = containerFactory.apply(eventloop, privKey);
					return container.start()
							.whenComplete(() -> pendingContainers.remove(pubKey))
							.whenResult($2 -> containers.put(pubKey, new Tuple2<>(privKey, container)))
							.map($2 -> container);
				})
				.whenComplete(toLogger(logger, thisMethod(), pubKey));
	}

	private Promise<?> removeUserContainer(PubKey pubKey) {
		Tuple2<PrivKey, C> tuple = containers.remove(pubKey);
		return (tuple == null ? Promise.complete() : tuple.getValue2().stop())
				.whenComplete(toLogger(logger, thisMethod(), pubKey));
	}

	@Nullable
	public C getUserContainer(PubKey pubKey) {
		Tuple2<PrivKey, C> tuple = containers.get(pubKey);
		return tuple == null ? null : tuple.getValue2();
	}

	private Promise<Void> sync() {
		return keyManager.getKeys()
				.then(expected -> {
					Map<PubKey, PrivKey> keysMap = new HashMap<>();
					for (PrivKey privKey : expected) {
						PubKey pubKey = privKey.computePubKey();
						keysMap.put(pubKey, privKey);
					}
					Set<PubKey> containerKeys = containers.keySet();
					Set<PubKey> toRemove = difference(containerKeys, keysMap.keySet());
					Set<PubKey> toAdd = difference(keysMap.keySet(), containerKeys);
					return Promises.all(Stream.concat(
							toRemove.stream().map(this::removeUserContainer),
							toAdd.stream().map(pubKey -> ensureUserContainer(keysMap.get(pubKey)))))
							.then($ -> keyManager.updateKeys(containers.values().stream()
									.map(Tuple2::getValue1)
									.collect(Collectors.toSet())));
				})
				.whenComplete(toLogger(logger, thisMethod()));
	}
}
