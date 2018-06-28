package io.datakernel.cube.service;

import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.cube.Utils.chunksInDiffs;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.CollectionUtils.toLimitedString;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;

public final class CubeBackupController<K, D, C> implements EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTAlgorithms<K, D> algorithms;
	private final OTRemoteEx<K, D> remote;
	private final LocalFsChunkStorage<C> storage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private final StageStats stageBackup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageBackupDb = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageBackupChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			OTRemoteEx<K, D> remote, LocalFsChunkStorage<C> storage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.algorithms = algorithms;
		this.remote = remote;
		this.storage = storage;
	}

	public static <K, D, C> CubeBackupController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			LocalFsChunkStorage<C> storage) {
		return new CubeBackupController<>(eventloop, cubeDiffScheme, algorithms, (OTRemoteEx<K, D>) algorithms.getRemote(), storage);
	}

	private final AsyncCallable<Void> backup = sharedCall(this::backupHead);

	public Stage<Void> backup() {
		return backup.call();
	}

	public Stage<Void> backupHead() {
		return remote.getHeads()
				.thenCompose(heads -> {
					if (heads.isEmpty()) {
						return Stage.ofException(new IllegalArgumentException("heads is empty"));
					}
					return backup(first(heads));
				})
				.whenComplete(stageBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Stage<Void> backup(K commitId) {
		return Stages.toTuple(remote.loadCommit(commitId), algorithms.checkout(commitId))
				.thenCompose(tuple -> Stages.runSequence(
						(AsyncCallable<Void>) () -> backupChunks(commitId, chunksInDiffs(cubeDiffScheme, tuple.getValue2())),
						(AsyncCallable<Void>) () -> backupDb(tuple.getValue1(), tuple.getValue2())))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	private Stage<Void> backupChunks(K commitId, Set<C> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds)
				.whenComplete(stageBackupChunks.recordStats())
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), chunkIds) :
						toLogger(logger, thisMethod(), toLimitedString(chunkIds, 6)));
	}

	private Stage<Void> backupDb(OTCommit<K, D> commit, List<D> snapshot) {
		return remote.backup(commit, snapshot)
				.whenComplete(stageBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), commit, snapshot));
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxOperation
	public void backupNow() {
		backup();
	}

	@JmxAttribute
	public StageStats getStageBackup() {
		return stageBackup;
	}

	@JmxAttribute
	public StageStats getStageBackupDb() {
		return stageBackupDb;
	}

	@JmxAttribute
	public StageStats getStageBackupChunks() {
		return stageBackupChunks;
	}

}

