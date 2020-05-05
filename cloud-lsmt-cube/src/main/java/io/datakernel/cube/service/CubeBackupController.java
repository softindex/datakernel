package io.datakernel.cube.service;

import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.cube.ot.CubeDiffScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxBeanEx;
import io.datakernel.jmx.api.attribute.JmxAttribute;
import io.datakernel.jmx.api.attribute.JmxOperation;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.repository.OTRepositoryEx;
import io.datakernel.ot.system.OTSystem;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.thisMethod;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.common.collection.CollectionUtils.toLimitedString;
import static io.datakernel.cube.Utils.chunksInDiffs;
import static io.datakernel.ot.OTAlgorithms.checkout;

public final class CubeBackupController<K, D, C> implements EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTRepositoryEx<K, D> repository;
	private final RemoteFsChunkStorage<C> storage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTRepositoryEx<K, D> repository,
			OTSystem<D> otSystem,
			RemoteFsChunkStorage<C> storage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.otSystem = otSystem;
		this.repository = repository;
		this.storage = storage;
	}

	public static <K, D, C> CubeBackupController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTRepositoryEx<K, D> otRepository,
			OTSystem<D> otSystem,
			RemoteFsChunkStorage<C> storage) {
		return new CubeBackupController<>(eventloop, cubeDiffScheme, otRepository, otSystem, storage);
	}

	private final AsyncSupplier<Void> backup = reuse(this::backupHead);

	public Promise<Void> backup() {
		return backup.get();
	}

	public Promise<Void> backupHead() {
		return repository.getHeads()
				.then(heads -> {
					if (heads.isEmpty()) {
						return Promise.ofException(new IllegalArgumentException("heads is empty"));
					}
					return backup(first(heads));
				})
				.whenComplete(promiseBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Promise<Void> backup(K commitId) {
		return Promises.toTuple(repository.loadCommit(commitId), checkout(repository, otSystem, commitId))
				.then(tuple -> Promises.sequence(
						() -> backupChunks(commitId, chunksInDiffs(cubeDiffScheme, tuple.getValue2())),
						() -> backupDb(tuple.getValue1(), tuple.getValue2())))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	private Promise<Void> backupChunks(K commitId, Set<C> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds)
				.whenComplete(promiseBackupChunks.recordStats())
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), chunkIds) :
						toLogger(logger, thisMethod(), toLimitedString(chunkIds, 6)));
	}

	private Promise<Void> backupDb(OTCommit<K, D> commit, List<D> snapshot) {
		return repository.backup(commit, snapshot)
				.whenComplete(promiseBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), commit, snapshot));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxOperation
	public void backupNow() {
		backup();
	}

	@JmxAttribute
	public PromiseStats getPromiseBackup() {
		return promiseBackup;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackupDb() {
		return promiseBackupDb;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackupChunks() {
		return promiseBackupChunks;
	}

}

