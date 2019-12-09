package io.global.ot.client;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.async.service.EventloopService;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.promise.Promises.until;

public final class MergeService<K, D> implements EventloopService {
	public static final Duration DEFAULT_INITIAL_DELAY = ApplicationSettings.getDuration(MergeService.class, "initialDelay", Duration.ofMillis(100));
	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

	private final Eventloop eventloop;
	private final OTRepository<K, D> repository;
	private final OTSystem<D> otSystem;
	private final AsyncSupplier<Set<K>> headsSupplier;

	private long initialDelay = DEFAULT_INITIAL_DELAY.toMillis();
	private long delay = initialDelay;
	private boolean stopped;

	private MergeService(Eventloop eventloop, OTRepository<K, D> repository, OTSystem<D> otSystem) {
		this.eventloop = eventloop;
		this.repository = repository;
		this.otSystem = otSystem;
		this.headsSupplier = repository.pollHeads();
	}

	public static <K, D> MergeService<K, D> create(Eventloop eventloop, OTRepository<K, D> repository, OTSystem<D> otSystem) {
		return new MergeService<>(eventloop, repository, otSystem);
	}

	public MergeService<K, D> withInitialDelay(Duration initialDelay) {
		this.initialDelay = initialDelay.toMillis();
		return this;
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		until((Void) null, $2 -> sync(), $2 -> stopped);
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<?> stop() {
		stopped = true;
		return Promise.complete();
	}

	private Promise<Void> sync() {
		return headsSupplier.get()
				.then(heads -> {
					if (heads.size() == 1) {
						return Promise.complete();
					}
					return Promises.delay(RANDOM.nextLong(delay/2, delay))
							.then($ -> repository.getHeads())
							.then(newHeads -> {
								if (newHeads.size() == 1) {
									delay = initialDelay;
									return Promise.complete();
								}
								return mergeHeads(heads)
										.then(mergeNotEmpty -> {
											if (mergeNotEmpty) {
												delay = initialDelay;
											} else {
												delay *= 2;
											}
											return Promise.complete();
										});
							});
				});
	}

	// true if merge is not empty
	private Promise<Boolean> mergeHeads(Set<K> heads) {
		return OTAlgorithms.merge(repository, otSystem, heads)
				.then(mergeCommit -> repository.pushAndUpdateHead(mergeCommit)
						.map($ -> !mergeCommit.getParents().values().stream()
								.flatMap(Collection::stream)
								.allMatch(otSystem::isEmpty)));
	}
}
