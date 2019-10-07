package io.global.ot.service;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.EventloopTaskScheduler.Schedule;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.transformMapValues;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toSet;

public final class ContainerManagerImpl<C extends UserContainer> implements ContainerManager<C>, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(ContainerManagerImpl.class);

	private static final Schedule DEFAULT_SYNC_SCHEDULE = Schedule.ofInterval(Duration.ofSeconds(5));

	private final Eventloop eventloop;
	private final BiFunction<Eventloop, PrivKey, C> containerFactory;

	private final Map<String, Promise<C>> pendingContainers = new HashMap<>();
	private final Map<String, C> containers = new HashMap<>();

	private final EventloopTaskScheduler synchronizer;
	private final KeyExchanger keyExchanger;

	private ContainerManagerImpl(Eventloop eventloop, KeyExchanger keyExchanger, BiFunction<Eventloop, PrivKey, C> containerFactory) {
		this.eventloop = eventloop;
		this.containerFactory = containerFactory;
		this.keyExchanger = keyExchanger;
		this.synchronizer = EventloopTaskScheduler.create(eventloop, this::sync)
				.withSchedule(DEFAULT_SYNC_SCHEDULE);
	}

	public static <C extends UserContainer> ContainerManagerImpl<C> create(Eventloop eventloop, KeyExchanger keyExchanger, BiFunction<Eventloop, PrivKey, C> containerFactory) {
		return new ContainerManagerImpl<>(eventloop, keyExchanger, containerFactory);
	}

	public ContainerManagerImpl<C> withSyncSchedule(Schedule schedule) {
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
						.map(EventloopService::stop))
						.whenResult($2 -> containers.clear())
				);
	}

	private Promise<C> ensureUserContainer(String id, PrivKey privKey) {
		C existing = containers.get(id);
		if (existing != null) {
			return Promise.of(existing);
		}
		return pendingContainers.computeIfAbsent(id,
				$ -> {
					C container = containerFactory.apply(eventloop, privKey);
					return container.start()
							.whenComplete(() -> pendingContainers.remove(id))
							.whenResult($2 -> containers.put(id, container))
							.map($2 -> container);
				})
				.whenComplete(toLogger(logger, thisMethod(), id));
	}

	private Promise<?> removeUserContainer(String id) {
		C container = containers.remove(id);
		return (container == null ? Promise.complete() : container.stop())
				.whenComplete(toLogger(logger, thisMethod(), id));
	}

	@Nullable
	public C getUserContainer(String id) {
		return containers.get(id);
	}

	@Override
	public boolean isSingleMode() {
		return false;
	}

	private Promise<Void> sync() {
		return keyExchanger.receiveKeys()
				.then(keysMap -> {
					Set<String> containerIds = containers.keySet();
					Set<String> toRemove = containers.entrySet().stream()
							.filter(entry -> {
								PrivKey existing = keysMap.get(entry.getKey());
								return existing == null || !existing.equals(entry.getValue().getKeys().getPrivKey());
							})
							.map(Map.Entry::getKey)
							.collect(toSet());
					Set<String> toAdd = difference(keysMap.keySet(), containerIds);
					return Promises.all(Stream.concat(
							toRemove.stream().map(this::removeUserContainer),
							toAdd.stream().map(id -> ensureUserContainer(id, keysMap.get(id)))))
							.then($ -> keyExchanger.sendKeys(transformMapValues(containers,
									container -> container.getKeys().getPrivKey())));
				})
				.whenComplete(toLogger(logger, TRACE, "sync"));
	}
}
