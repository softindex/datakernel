package io.global.ot.client;

import io.datakernel.async.*;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.AsyncExecutors.retry;
import static io.datakernel.async.Promises.repeat;
import static io.datakernel.util.CollectionUtils.difference;
import static io.global.util.Utils.eitherComplete;
import static java.util.Collections.singleton;

public final class RepoSynchronizer<D> implements EventloopService {
	public static final StacklessException SYNC_STOPPED = new StacklessException("Synchronization has been stopped");
	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry();

	private final Eventloop eventloop;
	private final OTDriver driver;
	private final OTSystem<D> otSystem;
	private final StructuredCodec<D> diffCodec;
	private final String repoName;
	private final KeyPair keys;
	private final Map<PubKey, SettablePromise<Set<CommitId>>> activeSyncs = new HashMap<>();

	private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

	private RepoSynchronizer(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem,
			StructuredCodec<D> diffCodec, String repoName, KeyPair keys) {
		this.eventloop = eventloop;
		this.driver = driver;
		this.otSystem = otSystem;
		this.diffCodec = diffCodec;
		this.repoName = repoName;
		this.keys = keys;
	}

	public static <D> RepoSynchronizer<D> create(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem,
			StructuredCodec<D> diffCodec, String repoName, KeyPair keys) {
		return new RepoSynchronizer<>(eventloop, driver, otSystem, diffCodec, repoName, keys);
	}

	public RepoSynchronizer<D> withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
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

		repeat(() -> eitherComplete(otherHeadsSupplier.get(), syncPromise)
				.then(otherHeads -> OTDriver.sync(repository, otSystem, otherHeads)))
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
	public MaterializedPromise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		HashSet<SettablePromise<Set<CommitId>>> syncPromises = new HashSet<>(activeSyncs.values());
		activeSyncs.clear();
		syncPromises.forEach(syncPromise -> syncPromise.setException(SYNC_STOPPED));
		return Promise.complete();
	}
}
