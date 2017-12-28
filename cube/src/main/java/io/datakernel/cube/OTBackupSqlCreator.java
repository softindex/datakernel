package io.datakernel.cube;

import com.google.gson.TypeAdapter;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTRemote;
import io.datakernel.ot.OTSystem;
import io.datakernel.utils.GsonAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static io.datakernel.ot.OTUtils.loadAllChanges;

public class OTBackupSqlCreator {
	public static final String DEFAULT_BACKUP_DB_NAME = "ot_revisions_backup";

	private final Logger logger = LoggerFactory.getLogger(OTBackupSqlCreator.class);
	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final LocalFsChunkStorage storage;
	private final OTRemote<Integer, LogDiff<CubeDiff>> otRemote;
	private final Comparator<Integer> otComparator;
	private final OTSystem<LogDiff<CubeDiff>> otSystem;
	private final DataSource dataSource;
	private final TypeAdapter<List<LogDiff<CubeDiff>>> adapter;
	private final String dbName;

	private OTBackupSqlCreator(Eventloop eventloop, ExecutorService executorService, LocalFsChunkStorage storage,
	                           OTRemote<Integer, LogDiff<CubeDiff>> otRemote, Comparator<Integer> otComparator,
	                           OTSystem<LogDiff<CubeDiff>> otSystem, DataSource dataSource,
	                           TypeAdapter<List<LogDiff<CubeDiff>>> adapter, String dbName) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.storage = storage;
		this.otRemote = otRemote;
		this.otComparator = otComparator;
		this.otSystem = otSystem;
		this.dataSource = dataSource;
		this.adapter = adapter;
		this.dbName = dbName;
	}

	public static OTBackupSqlCreator create(Eventloop eventloop, ExecutorService executorService, LocalFsChunkStorage storage,
	                                        OTRemote<Integer, LogDiff<CubeDiff>> otRemote, Comparator<Integer> otComparator,
	                                        OTSystem<LogDiff<CubeDiff>> otSystem, DataSource dataSource,
	                                        TypeAdapter<List<LogDiff<CubeDiff>>> adapter) {
		return new OTBackupSqlCreator(eventloop, executorService, storage, otRemote, otComparator,
				otSystem, dataSource, adapter, DEFAULT_BACKUP_DB_NAME);
	}

	public OTBackupSqlCreator withCustomDbName(String dbName) {
		return new OTBackupSqlCreator(eventloop, executorService, storage, otRemote, otComparator,
				otSystem, dataSource, adapter, dbName);
	}

	private String sql(String query) {
		return query.replace(DEFAULT_BACKUP_DB_NAME, dbName);
	}

	// TODO: add forceBackup method and JmxOperation for that
	public CompletionStage<Void> backup(Integer head) {
		return loadAllChanges(otRemote, otComparator, otSystem, head)
				.thenCompose(logDiffs -> backupChunks(head, collectChunkIds(logDiffs))
						.thenCompose($ -> backupDb(head, logDiffs)));
	}

	private static Set<Long> collectChunkIds(List<LogDiff<CubeDiff>> logDiffs) {
		return logDiffs.stream().flatMap(LogDiff::diffs).flatMap(CubeDiff::addedChunks).collect(Collectors.toSet());
	}

	private CompletionStage<Void> backupChunks(Integer commitId, Set<Long> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds);
	}

	private CompletionStage<Void> backupDb(Integer checkpointId, List<LogDiff<CubeDiff>> changes) {
		return eventloop.callConcurrently(executorService, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(
						sql("INSERT INTO ot_revisions_backup(id, snapshot) VALUES (?, ?)"))) {
					statement.setInt(1, checkpointId);
					statement.setString(2, GsonAdapters.toJson(adapter, changes));
					statement.executeUpdate();
					return null;
				}
			}
		});
	}
}

