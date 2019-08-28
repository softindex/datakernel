package io.global.forum;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.service.ContainerHolder;
import io.global.ot.service.UserContainer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.difference;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class ContainerEnsurer<C extends UserContainer> implements EventloopService {
	private final ContainerHolder<C> containerHolder;
	private final FsClient fsClient;
	private final String keysFile;
	private final EventloopTaskScheduler poller;

	private ContainerEnsurer(ContainerHolder<C> containerHolder, FsClient fsClient, String keysFile) {
		this.containerHolder = containerHolder;
		this.fsClient = fsClient;
		this.keysFile = keysFile;
		this.poller = EventloopTaskScheduler.create(containerHolder.getEventloop(), this::poll)
				.withInterval(Duration.ofSeconds(1));
	}

	public static <C extends UserContainer> ContainerEnsurer<C> create(ContainerHolder<C> containerHolder, FsClient fsClient, String keysFile) {
		return new ContainerEnsurer<>(containerHolder, fsClient, keysFile);
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return containerHolder.getEventloop();
	}

	@Override
	public @NotNull Promise<?> start() {
		return poller.start();
	}

	@Override
	public @NotNull Promise<?> stop() {
		return poller.stop();
	}

	private Promise<Void> poll() {
		return fsClient.download(keysFile)
				.then(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.then(buf -> {
					String[] keys = buf.asString(UTF_8).split("\n");
					Map<PubKey, PrivKey> keysMap = new HashMap<>();
					for (String key : keys) {
						try {
							PrivKey privKey = PrivKey.fromString(key);
							PubKey pubKey = privKey.computePubKey();
							keysMap.put(pubKey, privKey);
						} catch (ParseException e) {
							return Promise.ofException(e);
						}
					}
					Set<PubKey> containerKeys = containerHolder.getContainerKeys();
					Set<PubKey> toRemove = difference(containerKeys, keysMap.keySet());
					Set<PubKey> toAdd = difference(keysMap.keySet(), containerKeys);
					return Promises.all(Stream.concat(
							toRemove.stream().map(containerHolder::removeUserContainer),
							toAdd.stream().map(pubKey -> containerHolder.ensureUserContainer(keysMap.get(pubKey)))));
				});

	}
}
