package io.datakernel.cube.service;

import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepositoryEx;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.cube.Utils.chunksInDiffs;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.CollectionUtils.toLimitedString;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;

public final class CubeBackupController<K, D, C> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTAlgorithms<K, D> algorithms;
	private final OTRepositoryEx<K, D> repository;
	private final RemoteFsChunkStorage<C> storage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			OTRepositoryEx<K, D> repository, RemoteFsChunkStorage<C> storage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.algorithms = algorithms;
		this.repository = repository;
		this.storage = storage;
	}

	public static <K, D, C> CubeBackupController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			RemoteFsChunkStorage<C> storage) {
		return new CubeBackupController<>(eventloop, cubeDiffScheme, algorithms, (OTRepositoryEx<K, D>) algorithms.getRepository(), storage);
	}

	private final AsyncSupplier<Void> backup = reuse(this::backupHead);

	public Promise<Void> backup() {
		return backup.get();
	}

	public Promise<Void> backupHead() {
		return repository.getHeads()
				.thenCompose(heads -> {
					if (heads.isEmpty()) {
						return Promise.ofException(new IllegalArgumentException("heads is empty"));
					}
					return backup(first(heads));
				})
				.whenComplete(promiseBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Promise<Void> backup(K commitId) {
		return Promises.toTuple(repository.loadCommit(commitId), algorithms.checkout(commitId))
				.thenCompose(tuple -> Promises.runSequence(
						AsyncSupplier.cast(() -> backupChunks(commitId, chunksInDiffs(cubeDiffScheme, tuple.getValue2()))),
						AsyncSupplier.cast(() -> backupDb(tuple.getValue1(), tuple.getValue2()))))
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

