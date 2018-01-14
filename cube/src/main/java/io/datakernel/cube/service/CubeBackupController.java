package io.datakernel.cube.service;

import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stages;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTAlgorithms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.Stages.runSequence;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static java.util.Collections.max;
import static java.util.stream.Collectors.toSet;

public final class CubeBackupController implements EventloopJmxMBean {
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

	private final AsyncCallable<Void> backup = AsyncCallable.singleCallOf(this::backupHead);

	public CompletionStage<Void> backup() {
		return backup.call();
	}

	public CompletionStage<Void> backupHead() {
		return stageBackup.monitor(
				algorithms.getRemote().getHeads()
						.thenCompose(heads -> {
							if (heads.isEmpty())
								return Stages.ofException(new IllegalArgumentException("heads is empty"));
							return backup(max(heads));
						}));
	}

	@SuppressWarnings("unchecked")
	public CompletionStage<Void> backup(Integer commitId) {
		return algorithms.loadAllChanges(commitId)
				.thenCompose(logDiffs -> runSequence(
						() -> backupChunks(commitId, collectChunkIds(logDiffs)),
						() -> backupDb(commitId, logDiffs)));
	}

	private static Set<Long> collectChunkIds(List<LogDiff<CubeDiff>> logDiffs) {
		return logDiffs.stream().flatMap(LogDiff::diffs).flatMap(CubeDiff::addedChunks).collect(toSet());
	}

	private CompletionStage<Void> backupChunks(Integer commitId, Set<Long> chunkIds) {
		return stageBackupChunks.monitor(
				storage.backup(String.valueOf(commitId), chunkIds));
	}

	private CompletionStage<Void> backupDb(Integer commitId, List<LogDiff<CubeDiff>> diffs) {
		return stageBackupDb.monitor(
				algorithms.getRemote().backup(commitId, diffs));
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

	@JmxOperation
	public void resetStats() {
		this.stageBackup.resetStats();
		this.stageBackupDb.resetStats();
		this.stageBackupChunks.resetStats();
	}
}

