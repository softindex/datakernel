package io.global.ot.service;

import io.datakernel.async.service.EventloopService;
import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.PrivKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.service.EventloopTaskScheduler.Schedule;
import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.common.CollectorsEx.toMap;
import static java.util.stream.Collectors.toSet;

public final class ContainerManagerImpl<C extends UserContainer> implements ContainerManager<C>, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(ContainerManagerImpl.class);

	private static final Scope CONTAINER_SCOPE = Scope.of(ContainerScope.class);

	public static final Schedule DEFAULT_SYNC_SCHEDULE = Schedule.ofInterval(Duration.ofSeconds(5));

	private final Injector injector;
	private final Key<C> containerKey;
	private final Eventloop eventloop;

	private final Map<PrivKey, Promise<C>> containers = new HashMap<>();
	private final Map<String, PrivKey> idMapping = new HashMap<>();

	private final EventloopTaskScheduler synchronizer;
	private final KeyExchanger keyExchanger;

	private ContainerManagerImpl(Injector injector, Key<C> containerKey, Eventloop eventloop, KeyExchanger keyExchanger) {
		this.injector = injector;
		this.containerKey = containerKey;
		this.eventloop = eventloop;
		this.keyExchanger = keyExchanger;
		this.synchronizer = EventloopTaskScheduler.create(eventloop, this::sync)
				.withSchedule(DEFAULT_SYNC_SCHEDULE);
	}

	public static <C extends UserContainer> ContainerManagerImpl<C> create(Injector injector, Key<C> containerKey, Eventloop eventloop, KeyExchanger keyExchanger) {
		return new ContainerManagerImpl<>(injector, containerKey, eventloop, keyExchanger);
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
				.then($ -> Promises.all(containers.values().stream().map(p -> p.then(UserContainer::stop))))
				.whenResult($2 -> containers.clear());
	}

	private Promise<C> ensureUserContainer(String id, PrivKey privKey) {
		Promise<C> existing = containers.get(privKey);
		if (existing != null) {
			idMapping.put(id, privKey);
			return existing;
		}

		Injector subinjector = injector.enterScope(CONTAINER_SCOPE);
		subinjector.putInstance(PrivKey.class, privKey);

		C container = subinjector.getInstance(containerKey);

		Promise<C> promise = container.start()
				.whenResult($2 -> idMapping.put(id, privKey))
				.map($2 -> container);

		containers.put(privKey, promise);

		return promise.whenComplete(toLogger(logger, "ensureUserContainer", id));
	}

	private Promise<?> removeUserContainer(String id) {
		PrivKey privKey = idMapping.remove(id);
		if (privKey == null || idMapping.containsValue(privKey)) {
			return Promise.complete();
		}
		return containers.remove(privKey)
				.then(UserContainer::stop)
				.whenComplete(toLogger(logger, "removeUserContainer", id));
	}

	@Nullable
	public C getUserContainer(String id) {
		Promise<C> promise = containers.get(idMapping.get(id));
		return promise != null ? promise.getResult() : null;
	}

	@Override
	public boolean isSingleMode() {
		return false;
	}

	private Promise<Void> sync() {
		return keyExchanger.receiveKeys()
				.then(keysMap -> {
					Map<String, PrivKey> toAdd = keysMap.entrySet().stream()
							.filter(entry -> !idMapping.containsKey(entry.getKey()))
							.collect(toMap());
					Set<String> toRemove = idMapping.entrySet().stream()
							.filter(entry -> {
								PrivKey expected = keysMap.get(entry.getKey());
								return expected == null || !expected.equals(entry.getValue());
							})
							.map(Map.Entry::getKey)
							.collect(toSet());
					return Promises.all(toAdd.entrySet()
							.stream()
							.map(entry -> ensureUserContainer(entry.getKey(), entry.getValue())))
							.then($ -> Promises.all(toRemove.stream()
									.map(this::removeUserContainer)))
							.then($ -> keyExchanger.sendKeys(idMapping));
				})
				.whenComplete(toLogger(logger, TRACE, "sync"));
	}
}
