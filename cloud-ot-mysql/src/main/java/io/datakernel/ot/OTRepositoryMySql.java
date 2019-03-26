/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.indent;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.SqlUtils.execute;
import static io.datakernel.util.Utils.loadResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class OTRepositoryMySql<D> implements OTRepositoryEx<Long, D>, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	public static final Duration DEFAULT_DELETE_MARGIN = Duration.ofHours(1);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	public static final String DEFAULT_REVISION_TABLE = "ot_revisions";
	public static final String DEFAULT_DIFFS_TABLE = "ot_diffs";
	public static final String DEFAULT_BACKUP_TABLE = "ot_revisions_backup";

	private final Eventloop eventloop;
	private final Executor executor;
	private final OTSystem<D> otSystem;

	private final DataSource dataSource;
	private final StructuredCodec<List<D>> diffsCodec;

	private Duration deleteMargin = DEFAULT_DELETE_MARGIN;

	private String tableRevision = DEFAULT_REVISION_TABLE;
	private String tableDiffs = DEFAULT_DIFFS_TABLE;
	@Nullable
	private String tableBackup = DEFAULT_BACKUP_TABLE;

	private String createdBy = null;

	private final PromiseStats promiseCreateCommitId = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promisePush = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseGetHeads = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseLoadCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseIsSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseUpdateHeads = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseLoadSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseSaveSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private OTRepositoryMySql(Eventloop eventloop, Executor executor, OTSystem<D> otSystem, StructuredCodec<List<D>> diffsCodec,
			DataSource dataSource) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.otSystem = otSystem;
		this.dataSource = dataSource;
		this.diffsCodec = diffsCodec;
	}

	public static <D> OTRepositoryMySql<D> create(Eventloop eventloop, Executor executor, DataSource dataSource, OTSystem<D> otSystem, StructuredCodec<D> diffCodec) {
		StructuredCodec<List<D>> listCodec = indent(ofList(diffCodec), "\t");
		return new OTRepositoryMySql<>(eventloop, executor, otSystem, listCodec, dataSource);
	}

	public OTRepositoryMySql<D> withDeleteMargin(Duration deleteMargin) {
		this.deleteMargin = deleteMargin;
		return this;
	}

	public OTRepositoryMySql<D> withCreatedBy(String createdBy) {
		this.createdBy = createdBy;
		return this;
	}

	public OTRepositoryMySql<D> withCustomTableNames(String tableRevision, String tableDiffs, @Nullable String tableBackup) {
		this.tableRevision = tableRevision;
		this.tableDiffs = tableDiffs;
		this.tableBackup = tableBackup;
		return this;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public StructuredCodec<List<D>> getDiffsCodec() {
		return diffsCodec;
	}

	private String sql(String sql) {
		return sql
				.replace("{revisions}", tableRevision)
				.replace("{diffs}", tableDiffs)
				.replace("{backup}", Objects.toString(tableBackup, ""));
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		execute(dataSource, sql(new String(loadResource("sql/ot_diffs.sql"), UTF_8)));
		execute(dataSource, sql(new String(loadResource("sql/ot_revisions.sql"), UTF_8)));
		if (tableBackup != null) {
			execute(dataSource, sql(new String(loadResource("sql/ot_revisions_backup.sql"), UTF_8)));
		}
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql("TRUNCATE TABLE {diffs}"));
			statement.execute(sql("TRUNCATE TABLE {revisions}"));
		}
	}

	public Promise<Long> createCommitId() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(true);
						try (PreparedStatement statement = connection.prepareStatement(
								sql("INSERT INTO {revisions}(`type`, `created_by`, `level`) VALUES (?, ?, 0)"),
								Statement.RETURN_GENERATED_KEYS)) {
							statement.setString(1, "NEW");
							statement.setString(2, createdBy);
							statement.executeUpdate();
							ResultSet generatedKeys = statement.getGeneratedKeys();
							generatedKeys.next();
							return generatedKeys.getLong(1);
						}
					}
				})
				.whenComplete(promiseCreateCommitId.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public Promise<OTCommit<Long, D>> createCommit(Map<Long, ? extends List<? extends D>> parentDiffs, long level) {
		return createCommitId()
				.map(newId -> OTCommit.of(newId, parentDiffs, level));
	}

	private String toJson(List<D> diffs) {
		return JsonUtils.toJson(diffsCodec, diffs);
	}

	private List<D> fromJson(String json) throws ParseException {
		return JsonUtils.fromJson(diffsCodec, json);
	}

	@Override
	public Promise<Void> push(Collection<OTCommit<Long, D>> commits) {
		if (commits.isEmpty()) return Promise.complete();
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						try (PreparedStatement ps = connection.prepareStatement(
								sql("INSERT IGNORE INTO {revisions}(`id`) VALUES " +
										Stream.generate(() -> "(?)").limit(commits.size())
												.collect(joining(", "))))) {
							int pos = 1;
							for (OTCommit<Long, D> commit : commits) {
								ps.setLong(pos++, commit.getId());
							}
							ps.executeUpdate();
						}

						for (OTCommit<Long, D> commit : commits) {
							for (Long parentId : commit.getParents().keySet()) {
								List<D> diff = commit.getParents().get(parentId);
								try (PreparedStatement ps = connection.prepareStatement(
										sql("INSERT INTO {diffs}(`revision_id`, `parent_id`, `diff`) VALUES (?, ?, ?)"))) {
									ps.setLong(1, commit.getId());
									ps.setLong(2, parentId);
									ps.setString(3, toJson(diff));
									ps.executeUpdate();
								}
							}

							try (PreparedStatement ps = connection.prepareStatement(
									sql("UPDATE {revisions} SET `level` = ? WHERE `id` = ?"))) {
								ps.setLong(1, commit.getLevel());
								ps.setLong(2, commit.getId());
								ps.executeUpdate();
							}
						}

						connection.commit();
					}
					return (Void) null;
				})
				.whenComplete(promisePush.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), commits.stream().map(OTCommit::toString).collect(toList())));
	}

	@NotNull
	@Override
	public Promise<Void> updateHeads(Set<Long> newHeads, Set<Long> excludedHeads) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						updateRevisions(newHeads, connection, "HEAD");
						updateRevisions(excludedHeads, connection, "INNER");

						connection.commit();
					}
					return (Void) null;
				})
				.whenComplete(promiseUpdateHeads.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), newHeads, excludedHeads));
	}

	@NotNull
	@Override
	public Promise<Set<Long>> getHeads() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(
								sql("SELECT `id` FROM {revisions} WHERE `type`='HEAD'"))) {
							ResultSet resultSet = ps.executeQuery();
							Set<Long> result = new HashSet<>();
							while (resultSet.next()) {
								long id = resultSet.getLong(1);
								result.add(id);
							}
							return result;
						}
					}
				})
				.whenComplete(promiseGetHeads.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public Promise<Optional<List<D>>> loadSnapshot(Long revisionId) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(
								sql("SELECT `snapshot` FROM {revisions} WHERE `id`=?"))) {
							ps.setLong(1, revisionId);
							ResultSet resultSet = ps.executeQuery();

							if (!resultSet.next()) return Optional.<List<D>>empty();

							String str = resultSet.getString(1);
							List<D> snapshot = str == null ? Collections.emptyList() : fromJson(str);
							return Optional.of(otSystem.squash(snapshot));
						}
					}
				})
				.whenComplete(promiseLoadSnapshot.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@Override
	public Promise<OTCommit<Long, D>> loadCommit(Long revisionId) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						Map<Long, List<D>> parentDiffs = new HashMap<>();

						long timestamp = 0;
						boolean snapshot = false;
						long level = 0L;

						try (PreparedStatement ps = connection.prepareStatement(
								sql("" +
										"SELECT {revisions}.`level`," +
										" {revisions}.`snapshot` IS NOT NULL AS `snapshot`," +
										" UNIX_TIMESTAMP({revisions}.`timestamp`) AS `timestamp`, " +
										"{diffs}.`parent_id`, {diffs}.`diff` " +
										"FROM {revisions} " +
										"LEFT JOIN {diffs} ON {diffs}.`revision_id`={revisions}.`id` " +
										"WHERE {revisions}.`id`=?"))) {
							ps.setLong(1, revisionId);
							ResultSet resultSet = ps.executeQuery();

							while (resultSet.next()) {
								level = resultSet.getLong(1);
								snapshot = resultSet.getBoolean(2);
								timestamp = resultSet.getLong(3) * 1000L;
								long parentId = resultSet.getLong(4);
								String diffString = resultSet.getString(5);
								if (diffString != null) {
									List<D> diff = fromJson(diffString);
									parentDiffs.put(parentId, diff);
								}
							}
						}

						if (timestamp == 0) {
							throw new IOException("No commit with id: " + revisionId);
						}

						return OTCommit.of(revisionId, parentDiffs, level)
								.withTimestamp(timestamp)
								.withSnapshotHint(snapshot);
					}
				})
				.whenComplete(promiseLoadCommit.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@Override
	public Promise<Void> saveSnapshot(Long revisionId, List<D> diffs) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						String snapshot = toJson(otSystem.squash(diffs));
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"UPDATE {revisions} " +
								"SET `snapshot` = ? " +
								"WHERE `id` = ?"))) {
							ps.setString(1, snapshot);
							ps.setLong(2, revisionId);
							ps.executeUpdate();
							return (Void) null;
						}
					}
				})
				.whenComplete(promiseSaveSnapshot.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId, diffs));
	}

	@Override
	public Promise<Void> cleanup(Long minId) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						try (PreparedStatement ps = connection.prepareStatement(
								sql("DELETE FROM {revisions} WHERE `timestamp` < " +
										"(SELECT `timestamp` - INTERVAL ? second FROM (SELECT `id`, `timestamp` FROM {revisions}) AS t WHERE `id`=?)"))) {
							ps.setLong(1, deleteMargin.getSeconds());
							ps.setLong(2, minId);
							ps.executeUpdate();
						}

						try (PreparedStatement ps = connection.prepareStatement(
								sql("DELETE FROM {diffs} WHERE NOT EXISTS (SELECT * FROM {revisions} WHERE {revisions}.`id`={diffs}.`revision_id`)"))) {
							ps.executeUpdate();
						}

						connection.commit();
					}

					return (Void) null;
				})
				.whenComplete(toLogger(logger, thisMethod(), minId));
	}

	@Override
	public Promise<Void> backup(OTCommit<Long, D> commit, List<D> snapshot) {
		checkNotNull(tableBackup, "Cannot backup when backup table is null");
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement statement = connection.prepareStatement(
								sql("INSERT INTO {backup}(`id`, `level`, `snapshot`) VALUES (?, ?, ?)"))) {
							statement.setLong(1, commit.getId());
							statement.setLong(2, commit.getLevel());
							statement.setString(3, toJson(snapshot));
							statement.executeUpdate();
							return (Void) null;
						}
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), commit.getId(), snapshot));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private void updateRevisions(Collection<Long> heads, Connection connection, String type) throws SQLException {
		if (heads.isEmpty()) return;
		try (PreparedStatement ps = connection.prepareStatement(
				sql("UPDATE {revisions} SET `type`=\"" + type + "\" WHERE `id` IN " + Stream.generate(() -> "?").limit(heads.size())
						.collect(joining(", ", "(", ")"))))) {
			int pos = 1;
			for (Long id : heads) {
				ps.setLong(pos++, id);
			}
			ps.executeUpdate();
		}
	}

	@JmxAttribute
	public PromiseStats getPromiseCreateCommitId() {
		return promiseCreateCommitId;
	}

	@JmxAttribute
	public PromiseStats getPromisePush() {
		return promisePush;
	}

	@JmxAttribute
	public PromiseStats getPromiseGetHeads() {
		return promiseGetHeads;
	}

	@JmxAttribute
	public PromiseStats getPromiseLoadCommit() {
		return promiseLoadCommit;
	}

	@JmxAttribute
	public PromiseStats getPromiseIsSnapshot() {
		return promiseIsSnapshot;
	}

	@JmxAttribute
	public PromiseStats getPromiseLoadSnapshot() {
		return promiseLoadSnapshot;
	}

	@JmxAttribute
	public PromiseStats getPromiseSaveSnapshot() {
		return promiseSaveSnapshot;
	}

}
