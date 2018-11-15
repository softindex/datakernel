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

import com.google.gson.TypeAdapter;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.json.GsonAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static io.datakernel.json.GsonAdapters.indent;
import static io.datakernel.json.GsonAdapters.ofList;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.stream.Collectors.*;

public class OTRepositorySql<D> implements OTRepositoryEx<Long, D>, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	public static final Duration DEFAULT_DELETE_MARGIN = Duration.ofHours(1);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	public static final String DEFAULT_REVISION_TABLE = "ot_revisions";
	public static final String DEFAULT_DIFFS_TABLE = "ot_diffs";
	public static final String DEFAULT_BACKUP_TABLE = "ot_revisions_backup";

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final OTSystem<D> otSystem;

	private final DataSource dataSource;
	private final TypeAdapter<List<D>> diffsAdapter;

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
	private final PromiseStats promiseLoadSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseSaveSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private OTRepositorySql(Eventloop eventloop, ExecutorService executor, OTSystem<D> otSystem, TypeAdapter<List<D>> diffsAdapter,
			DataSource dataSource) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.otSystem = otSystem;
		this.dataSource = dataSource;
		this.diffsAdapter = diffsAdapter;
	}

	public static <D> OTRepositorySql<D> create(Eventloop eventloop, ExecutorService executor, DataSource dataSource, OTSystem<D> otSystem, TypeAdapter<D> diffAdapter) {
		TypeAdapter<List<D>> listAdapter = indent(ofList(diffAdapter), "\t");
		return new OTRepositorySql<>(eventloop, executor, otSystem, listAdapter, dataSource);
	}

	public OTRepositorySql<D> withDeleteMargin(Duration deleteMargin) {
		this.deleteMargin = deleteMargin;
		return this;
	}

	public OTRepositorySql<D> withCreatedBy(String createdBy) {
		this.createdBy = createdBy;
		return this;
	}

	public OTRepositorySql<D> withCustomTableNames(String tableRevision, String tableDiffs, @Nullable String tableBackup) {
		this.tableRevision = tableRevision;
		this.tableDiffs = tableDiffs;
		this.tableBackup = tableBackup;
		return this;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public TypeAdapter<List<D>> getDiffsAdapter() {
		return diffsAdapter;
	}

	private String sql(String sql) {
		return sql
				.replace("{revisions}", tableRevision)
				.replace("{diffs}", tableDiffs)
				.replace("{backup}", Objects.toString(tableBackup, ""));
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
		return Promise.ofCallable(executor,
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
				.thenApply(newId -> OTCommit.of(newId, parentDiffs, level));
	}

	private String toJson(List<D> diffs) {
		return GsonAdapters.toJson(diffsAdapter, diffs);
	}

	@SuppressWarnings("unchecked")
	private List<D> fromJson(String json) throws ParseException {
		return GsonAdapters.fromJson(diffsAdapter, json);
	}

	@Override
	public Promise<Void> push(Collection<OTCommit<Long, D>> commits) {
		if (commits.isEmpty()) return Promise.complete();
		return Promise.ofCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						Set<Long> commitIds = commits.stream().map(OTCommit::getId).collect(toSet());
						Set<Long> commitsParentIds = commits.stream().flatMap(commit -> commit.getParents().keySet().stream()).collect(toSet());
						Set<Long> headCommitIds = difference(commitIds, commitsParentIds);
						Set<Long> innerCommitIds = union(commitsParentIds, difference(commitIds, headCommitIds));

						try (PreparedStatement ps = connection.prepareStatement(
								sql("INSERT IGNORE INTO {revisions}(`id`) VALUES " +
										Stream.generate(() -> "(?)").limit(commitIds.size())
												.collect(joining(", "))))) {
							int pos = 1;
							for (Long id : commitIds) {
								ps.setLong(pos++, id);
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
									sql("UPDATE {revisions} SET `level`=?, `type`=? WHERE `id`=?"))) {
								ps.setLong(1, commit.getLevel());
								ps.setString(2, headCommitIds.contains(commit.getId()) ? "HEAD" : "INNER");
								ps.setLong(3, commit.getId());
								ps.executeUpdate();
							}
						}

						if (!commitsParentIds.isEmpty()) {
							try (PreparedStatement ps = connection.prepareStatement(
									sql("UPDATE {revisions} SET `type`='INNER' WHERE `id` IN " +
											Stream.generate(() -> "?").limit(commitsParentIds.size())
													.collect(joining(", ", "(", ")"))))) {
								int pos = 1;
								for (Long id : commitsParentIds) {
									ps.setLong(pos++, id);
								}
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

	@Override
	public Promise<Set<Long>> getHeads() {
		return Promise.ofCallable(executor,
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
		return Promise.ofCallable(executor,
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
		return Promise.ofCallable(executor,
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
		return Promise.ofCallable(executor,
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
		return Promise.ofCallable(executor,
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
		checkState(this.tableBackup != null);
		return Promise.ofCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement statement = connection.prepareStatement(
								sql("INSERT INTO {backup}(`id`, `snapshot`) VALUES (?, ?)"))) {
							statement.setLong(1, commit.getId());
							statement.setString(2, toJson(snapshot));
							statement.executeUpdate();
							return (Void) null;
						}
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), commit.getId(), snapshot));
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
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
