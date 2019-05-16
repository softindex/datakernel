package io.global.fs.app.container;

import io.datakernel.async.*;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.local.GlobalFsDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.retry;
import static io.datakernel.async.Promises.repeat;
import static io.global.fs.api.GlobalFsCheckpoint.COMPARATOR;
import static io.global.util.Utils.eitherComplete;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class DirSynchronizer implements EventloopService {
	public static final StacklessException NOT_A_PARTICIPANT = new StacklessException(DirSynchronizer.class, "Not a participant");
	public static final StacklessException STOPPED_EXCEPTION = new StacklessException(FsMessagingService.class, "Service has been stopped");

	private final Eventloop eventloop;
	private final String dir;
	private final KeyPair keys;
	private final SimKey simKey;
	private final GlobalFsDriver driver;
	private final Set<PubKey> participants;
	private final SettablePromise<@Nullable Void> stopPromise = new SettablePromise<>();

	private DirSynchronizer(Eventloop eventloop, String dir, KeyPair keys, SimKey simKey, GlobalFsDriver driver, Set<PubKey> participants) {
		this.eventloop = eventloop;
		this.dir = dir;
		this.keys = keys;
		this.simKey = simKey;
		this.driver = driver;
		this.participants = participants;
	}

	public static DirSynchronizer create(Eventloop eventloop, String dir, KeyPair keys, SimKey simKey, GlobalFsDriver driver, Set<PubKey> participants) {
		return new DirSynchronizer(eventloop, dir, keys, simKey, driver, participants);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<?> start() {
		if (!participants.contains(keys.getPubKey())) {
			return Promise.ofException(NOT_A_PARTICIPANT);
		}
		sync();
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<?> stop() {
		stopPromise.trySetException(STOPPED_EXCEPTION);
		return Promise.complete();
	}

	private void sync() {
		participants.remove(keys.getPubKey());
		AsyncSupplier<Void> supplier = () -> getUpdates()
				.then(updateInfos -> Promises.all(updateInfos.stream().map(updateInfo ->
						Promises.all(updateInfo.getUpdates().stream().map(checkpoint -> {
							if (checkpoint.isTombstone()) {
								return driver.delete(keys, checkpoint.getFilename(), checkpoint.getRevision());
							} else {
								return ChannelSupplier.ofPromise(driver.download(updateInfo.getParticipant(), checkpoint.getFilename(), 0, -1, simKey))
										.streamTo(driver.upload(keys, checkpoint.getFilename(), 0, checkpoint.getRevision(), simKey));
							}
						})))));

		AsyncSupplier<Void> retryingSupplier = retry(supplier);

		repeat(() -> eitherComplete(retryingSupplier.get(), stopPromise));
	}

	private Promise<Set<UpdateInfo>> getUpdates() {
		return driver.listEntities(keys.getPubKey(), dir + "/**")
				.then(list -> {
					Set<UpdateInfo> result = new HashSet<>();
					Map<String, GlobalFsCheckpoint> localEntities = list.stream()
							.collect(toMap(GlobalFsCheckpoint::getFilename, Function.identity()));
					return Promises.all(participants.stream().map(pubKey -> driver.listEntities(pubKey, dir + "/**")
							.whenResult(peerEntities -> {
								Set<GlobalFsCheckpoint> updates = peerEntities.stream()
										.filter(checkpoint -> !localEntities.containsKey(checkpoint.getFilename()) ||
												COMPARATOR.compare(localEntities.get(checkpoint.getFilename()), checkpoint) < 0)
										.collect(toSet());
								if (!updates.isEmpty()) {
									result.add(new UpdateInfo(pubKey, updates));
								}
							})))
							.then($ -> result.isEmpty() ? Promises.delay(Promise.of(result), Duration.ofSeconds(2)) : Promise.of(result));
				});
	}

	private static class UpdateInfo {
		private final PubKey participant;
		private final Set<GlobalFsCheckpoint> updates;

		private UpdateInfo(PubKey participant, Set<GlobalFsCheckpoint> updates) {
			this.participant = participant;
			this.updates = updates;
		}

		public PubKey getParticipant() {
			return participant;
		}

		public Set<GlobalFsCheckpoint> getUpdates() {
			return updates;
		}
	}
}
