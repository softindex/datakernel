package io.datakernel.cube.service;

import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTAlgorithms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static java.util.Collections.max;
import static java.util.stream.Collectors.toSet;

public final class CubeBackupController implements EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;
	private final OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms;
	private final LocalFsChunkStorage storage;

	private final StageStats stageBackup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageBackupDb = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageBackupChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Eventloop eventloop, OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms, LocalFsChunkStorage storage) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
		this.storage = storage;
	}

	public static CubeBackupController create(Eventloop eventloop, OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms, LocalFsChunkStorage storage) {
		return new CubeBackupController(eventloop, algorithms, storage);
	}

	private final AsyncCallable<Void> backup = sharedCall(this::backupHead);

	public Stage<Void> backup() {
		return backup.call();
	}

	public Stage<Void> backupHead() {
		return algorithms.getRemote().getHeads()
				.thenCompose(heads -> {
					if (heads.isEmpty()) {
						return Stage.ofException(new IllegalArgumentException("heads is empty"));
					}
					return backup(max(heads));
				})
				.whenComplete(stageBackup.recordStats());
	}

	public Stage<Void> backup(Integer commitId) {
		return algorithms.checkout(commitId)
				.thenCompose(logDiffs -> Stages.runSequence(
						(AsyncCallable<Void>) () -> backupChunks(commitId, collectChunkIds(logDiffs)),
						(AsyncCallable<Void>) () -> backupDb(commitId, logDiffs)));
	}

	private static Set<Long> collectChunkIds(List<LogDiff<CubeDiff>> logDiffs) {
		return logDiffs.stream().flatMap(LogDiff::diffs).flatMap(CubeDiff::addedChunks).collect(toSet());
	}

	private Stage<Void> backupChunks(Integer commitId, Set<Long> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds)
				.whenComplete(stageBackupChunks.recordStats());
	}

	private Stage<Void> backupDb(Integer commitId, List<LogDiff<CubeDiff>> diffs) {
		return algorithms.getRemote().backup(commitId, diffs)
				.whenComplete(stageBackupDb.recordStats());
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

