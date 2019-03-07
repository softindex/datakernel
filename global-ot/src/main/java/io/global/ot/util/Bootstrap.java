package io.global.ot.util;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTCommit;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import org.jetbrains.annotations.NotNull;

import static java.util.Collections.*;

/**
 * Utility class that initializes repository with root commit and snapshot for this commit. If repository has already
 * been initialized, does nothing.
 *
 * @param <D> - diffs type parameter
 */
public final class Bootstrap<D> implements EventloopService {
	private final Eventloop eventloop;
	private final OTDriver driver;
	private final MyRepositoryId<D> myRepositoryId;

	public Bootstrap(Eventloop eventloop, OTDriver driver, MyRepositoryId<D> myRepositoryId) {
		this.eventloop = eventloop;
		this.driver = driver;
		this.myRepositoryId = myRepositoryId;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return driver.getHeads(myRepositoryId.getRepositoryId())
				.thenCompose(heads -> {
					if (!heads.isEmpty()) return Promise.complete();

					OTCommit<CommitId, D> rootCommit = driver.createCommit(myRepositoryId, emptyMap(), 1);
					return driver.push(myRepositoryId, rootCommit)
							.thenCompose($ -> driver.updateHeads(myRepositoryId, singleton(rootCommit.getId()), emptySet()))
							.thenCompose($ -> driver.saveSnapshot(myRepositoryId, rootCommit.getId(), emptyList()));
				})
				.materialize();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promise.complete();
	}

	public OTDriver getDriver() {
		return driver;
	}

	public MyRepositoryId<D> getMyRepositoryId() {
		return myRepositoryId;
	}
}
