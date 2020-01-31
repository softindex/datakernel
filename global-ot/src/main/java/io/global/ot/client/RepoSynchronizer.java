package io.global.ot.client;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.async.service.EventloopService;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.ref.RefLong;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.datakernel.promise.SettablePromise;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.async.process.AsyncExecutors.retry;
import static io.datakernel.common.collection.CollectionUtils.difference;
import static io.datakernel.promise.Promises.repeat;
import static io.global.util.Utils.eitherComplete;
import static java.util.Collections.singleton;

public final class RepoSynchronizer<D> implements EventloopService {
	public static final StacklessException SYNC_STOPPED = new StacklessException("Synchronization has been stopped");
	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(60));
	public static final Duration DEFAULT_INITIAL_DELAY = Duration.ofMillis(100);
	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

	private final Eventloop eventloop;
	private final OTDriver driver;
	private final OTSystem<D> otSystem;
	private final StructuredCodec<D> diffCodec;
	private final String repoName;
	private final KeyPair keys;
	private final Map<PubKey, SettablePromise<Set<CommitId>>> activeSyncs = new HashMap<>();

	private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;
	private Duration initialDelay = DEFAULT_INITIAL_DELAY;

	private RepoSynchronizer(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem, MyRepositoryId<D> myRepositoryId) {
		this.eventloop = eventloop;
		this.driver = driver;
		this.otSystem = otSystem;
		this.diffCodec = myRepositoryId.getDiffCodec();
		this.repoName = myRepositoryId.getRepositoryId().getName();
		this.keys = myRepositoryId.getPrivKey().computeKeys();
	}

	public static <D> RepoSynchronizer<D> create(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem, MyRepositoryId<D> myRepositoryId) {
		return new RepoSynchronizer<>(eventloop, driver, otSystem, myRepositoryId);
	}

	public RepoSynchronizer<D> withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	public RepoSynchronizer<D> withInitialDelay(Duration initialDelay){
		this.initialDelay = initialDelay;
		return this;
	}

	public void sync(Set<PubKey> others) {
		difference(activeSyncs.keySet(), others).forEach(this::stopSync);
		others.forEach(this::startSync);
	}

	private void startSync(PubKey other) {
		if (keys.getPubKey().equals(other)) return;

		RepoID otherRepo = RepoID.of(other, repoName);

		if (activeSyncs.containsKey(other)) return;

		OTRepository<CommitId, D> repository = getRepository(otherRepo);

		SettablePromise<Set<CommitId>> syncPromise = new SettablePromise<>();

		activeSyncs.put(other, syncPromise);

		AsyncSupplier<Set<CommitId>> otherHeadsSupplier = driver.pollHeads(otherRepo)
				.withExecutor(retry(retryPolicy));

		RefLong pollDelay = new RefLong(initialDelay.toMillis());
		repeat(() -> eitherComplete(syncPromise, otherHeadsSupplier.get())
				.then(otherHeads -> OTDriver.sync(repository, otSystem, otherHeads)
						.then(mergeNotEmpty -> {
							if (mergeNotEmpty) {
								pollDelay.value = initialDelay.toMillis();
								return Promise.complete();
							} else {
								return Promises.delay(RANDOM.nextLong(pollDelay.value))
										.whenResult($ -> pollDelay.value *= 2);
							}
						})))
				.whenException(e -> activeSyncs.remove(other));
	}

	private void stopSync(PubKey other) {
		if (other.equals(keys.getPubKey())) return;

		SettablePromise<Set<CommitId>> syncPromise = activeSyncs.remove(other);
		if (syncPromise != null) {
			syncPromise.trySetException(SYNC_STOPPED);
		}
	}

	private OTRepository<CommitId, D> getRepository(RepoID otherRepo) {
		RepoID initiatorRepo = RepoID.of(keys, repoName);
		MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(initiatorRepo, keys.getPrivKey(), diffCodec);
		return new OTRepositoryAdapter<>(driver, myRepositoryId, singleton(otherRepo));
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
		HashSet<SettablePromise<Set<CommitId>>> syncPromises = new HashSet<>(activeSyncs.values());
		activeSyncs.clear();
		syncPromises.forEach(syncPromise -> syncPromise.setException(SYNC_STOPPED));
		return Promise.complete();
	}
}
