package io.datakernel.ot;

import com.google.gson.TypeAdapter;
import io.datakernel.annotation.Nullable;
import io.datakernel.utils.GsonAdapters;
import io.datakernel.utils.JsonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.utils.GsonAdapters.indent;
import static io.datakernel.utils.GsonAdapters.ofList;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class OTRemoteSql<D> implements OTRemote<Integer, D> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final String TABLE_REVISION = "ot_revision";
	public static final String TABLE_DIFFS = "ot_diffs";

	private final ExecutorService executor;
	private final OTSystem<D> otSystem;

	private final DataSource dataSource;
	private final TypeAdapter<List<D>> diffsAdapter;
	private final TypeAdapter<Map<Integer, List<D>>> mapAdapter;

	private final String tableRevision;
	private final String tableDiffs;
	private final String createdBy;

	private OTRemoteSql(ExecutorService executor, OTSystem<D> otSystem, TypeAdapter<List<D>> diffsAdapter,
	                    TypeAdapter<Map<Integer, List<D>>> mapAdapter, DataSource dataSource,
	                    String tableRevision, String tableDiffs, String createdBy) {
		this.executor = executor;
		this.otSystem = otSystem;
		this.dataSource = dataSource;
		this.diffsAdapter = diffsAdapter;
		this.mapAdapter = mapAdapter;
		this.tableRevision = tableRevision;
		this.tableDiffs = tableDiffs;
		this.createdBy = createdBy;
	}

	public static <D> OTRemoteSql<D> create(ExecutorService executor, DataSource dataSource, OTSystem<D> otSystem, TypeAdapter<D> diffAdapter) {
		final TypeAdapter<List<D>> listAdapter = indent(ofList(diffAdapter), "\t");

		final TypeAdapter<Map<Integer, List<D>>> mapDiffsAdapter = GsonAdapters.transform(GsonAdapters.ofMap(listAdapter),
				value -> value.entrySet().stream().collect(Collectors.toMap(entry -> Integer.parseInt(entry.getKey()), Map.Entry::getValue)),
				value -> value.entrySet().stream().collect(Collectors.toMap(entry -> String.valueOf(entry.getKey()), Map.Entry::getValue)));
		return new OTRemoteSql<>(executor, otSystem, listAdapter, mapDiffsAdapter, dataSource, TABLE_REVISION, TABLE_DIFFS, null);
	}

	public OTRemoteSql<D> withCreatedBy(String createdBy) {
		return new OTRemoteSql<>(executor, otSystem, diffsAdapter, mapAdapter, dataSource, tableRevision, tableDiffs, createdBy);
	}

	public OTRemoteSql<D> withCustomTableNames(String tableRevision, String tableDiffs, String tableMerges) {
		return new OTRemoteSql<>(executor, otSystem, diffsAdapter, mapAdapter, dataSource, tableRevision, tableDiffs, createdBy);
	}

	private String sql(String sql) {
		return sql.replace(TABLE_REVISION, tableRevision)
				.replace(TABLE_DIFFS, tableDiffs);
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql("TRUNCATE TABLE ot_diffs"));
			statement.execute(sql("TRUNCATE TABLE ot_revisions"));
			statement.execute(sql("TRUNCATE TABLE ot_merges"));
		}
	}

	@Override
	public CompletionStage<Integer> createId() {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Start Create id");
			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(true);
				try (PreparedStatement statement = connection.prepareStatement(
						sql("INSERT INTO ot_revisions(type, created_by) VALUES (?, ?)"),
						Statement.RETURN_GENERATED_KEYS)) {
					statement.setString(1, "NEW");
					statement.setString(2, createdBy);
					statement.executeUpdate();
					ResultSet generatedKeys = statement.getGeneratedKeys();
					generatedKeys.next();
					final int id = generatedKeys.getInt(1);
					logger.trace("Id created: {}", id);
					return id;
				}
			}
		});
	}

	private String toJson(List<D> diffs) throws JsonException {
		return GsonAdapters.toJson(diffsAdapter, diffs);
	}

	@SuppressWarnings("unchecked")
	private List<D> fromJson(String json) throws JsonException {
		return GsonAdapters.fromJson(diffsAdapter, json);
	}

	public CompletionStage<Void> push(OTCommit<Integer, D> commit) {
		return push(singletonList(commit));
	}

	@Override
	public CompletionStage<Void> push(List<OTCommit<Integer, D>> commits) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Push {} commits: {}", commits.size(),
					commits.stream().map(OTCommit::idsToString).collect(toList()));

			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(false);

				for (int i = 0; i < commits.size(); i++) {
					OTCommit<Integer, D> commit = commits.get(i);
					try (PreparedStatement ps = connection.prepareStatement(sql(
							"UPDATE ot_revisions SET `type`=? WHERE `id`=?"))) {
						ps.setString(1, (i == commits.size() - 1) ? "HEAD" : "INNER");
						ps.setInt(2, commit.getId());
						ps.executeUpdate();
					}

					for (Map.Entry<Integer, List<D>> entry : commit.getParents().entrySet()) {
						Integer parentId = entry.getKey();
						List<D> diff = entry.getValue();

						try (PreparedStatement ps = connection.prepareStatement(
								sql("INSERT INTO ot_diffs(revision_id, parent_id, diff) VALUES (?, ?, ?)"))) {
							ps.setInt(1, commit.getId());
							ps.setInt(2, parentId);
							ps.setString(3, toJson(diff));
							ps.executeUpdate();
						}
					}
				}

				final Set<Integer> parents = commits.get(0).getParents().keySet();
				final String args = nCopies(parents.size(), "?").stream().collect(Collectors.joining(","));

				if (!parents.isEmpty()) {
					try (PreparedStatement ps = connection.prepareStatement(
							sql("UPDATE ot_revisions SET type='INNER' WHERE  id IN (" + args + ")"))) {
						int pos = 1;
						for (Integer parent : parents) {
							ps.setInt(pos++, parent);
						}
						ps.executeUpdate();
					}
				}

				connection.commit();
				logger.trace("{} commits pushed: {}", commits.size(),
						commits.stream().map(OTCommit::idsToString).collect(toList()));
			}
			return null;
		});
	}

	@Override
	public CompletionStage<Set<Integer>> getHeads() {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Get Heads");
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(
						sql("SELECT id FROM ot_revisions WHERE type='HEAD'"))) {
					ResultSet resultSet = ps.executeQuery();
					Set<Integer> result = new HashSet<>();
					while (resultSet.next()) {
						int id = resultSet.getInt(1);
						result.add(id);
					}
					logger.trace("Current heads: {}", result);
					return result;
				}
			}
		});
	}

	@Override
	public CompletionStage<List<D>> loadSnapshot(Integer revisionId) {
		logger.trace("Load snapshot: {}", revisionId);
		return getCurrentEventloop().callConcurrently(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(sql("" +
						"SELECT snapshot " +
						"FROM ot_revisions " +
						"WHERE id = ? " +
						"AND snapshot is not null "))) {
					ps.setInt(1, revisionId);
					final ResultSet resultSet = ps.executeQuery();

					if (!resultSet.next()) throw new IllegalArgumentException("No snapshot for id: " + revisionId);

					final List<D> snapshot = fromJson(resultSet.getString(1));
					logger.trace("Snapshot loaded: {}", revisionId);
					return otSystem.squash(snapshot);
				}
			}
		});
	}

	@Override
	public CompletionStage<Boolean> isSnapshot(Integer revisionId) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Start is snapshot: {}", revisionId);
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(sql("" +
						"SELECT COUNT(*) " +
						"FROM ot_revisions " +
						"WHERE id = ? " +
						"AND snapshot is not null"))) {
					ps.setInt(1, revisionId);
					ResultSet resultSet = ps.executeQuery();

					resultSet.next();
					final boolean result = resultSet.getInt(1) == 1;
					logger.trace("is Snapshot finished: {}, {}", revisionId, result);
					return result;
				}
			}
		});
	}

	@Override
	public CompletionStage<OTCommit<Integer, D>> loadCommit(Integer revisionId) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Start load commit: {}", revisionId);
			try (Connection connection = dataSource.getConnection()) {
				final Map<Integer, List<D>> parentDiffs = new HashMap<>();

				try (PreparedStatement ps = connection.prepareStatement(
						sql("SELECT parent_id, diff FROM ot_diffs WHERE revision_id=?"))) {
					ps.setInt(1, revisionId);
					ResultSet resultSet = ps.executeQuery();
					while (resultSet.next()) {
						int parentId = resultSet.getInt(1);
						String diffString = resultSet.getString(2);
						List<D> diff = fromJson(diffString);
						parentDiffs.put(parentId, diff);
					}
				}

				if (parentDiffs.isEmpty()) {
					try (PreparedStatement ps = connection.prepareStatement(
							sql("SELECT COUNT(*) FROM ot_revisions WHERE id=?"))) {
						ps.setInt(1, revisionId);
						ResultSet resultSet = ps.executeQuery();
						resultSet.next();
						if (resultSet.getInt(1) == 0) {
							throw new IllegalArgumentException("No commit with id: " + revisionId);
						}
					}
				}

				logger.trace("Finish load commit: {}, parentIds: {}", revisionId, parentDiffs.keySet());
				return OTCommit.of(revisionId, parentDiffs);
			}
		});
	}

	@Override
	public CompletionStage<Void> saveSnapshot(Integer revisionId, List<D> diffs) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Start save snapshot: {}, diffs: {}", revisionId, diffs.size());
			final String snapshot = toJson(otSystem.squash(diffs));
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(sql("" +
						"UPDATE ot_revisions " +
						"SET snapshot = ? " +
						"WHERE id = ?"))) {
					ps.setString(1, snapshot);
					ps.setInt(2, revisionId);
					ps.executeUpdate();
					logger.trace("Finish save snapshot: {}, diffs: {}", revisionId, diffs.size());
					return null;
				}
			}
		});
	}

	public CompletionStage<Timestamp> timestamp(Integer revisionId) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Start timestamp, revision id :{}", revisionId);
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(sql("" +
						"SELECT timestamp " +
						"FROM ot_revisions " +
						"WHERE id = ?"))) {
					ps.setInt(1, revisionId);
					final ResultSet resultSet = ps.executeQuery();

					if (!resultSet.next()) {
						throw new IllegalArgumentException("No revision with id: " + revisionId);
					}
					final Timestamp timestamp = Timestamp.valueOf(resultSet.getString(1));
					logger.trace("Finish timestamp, revision id: {}, timestamp: {}", revisionId, timestamp);
					return timestamp;
				}
			}
		});
	}

	public CompletionStage<Void> cleanup(Integer minId) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Start cleanup: {}", minId);
			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(false);

				try (PreparedStatement ps = connection.prepareStatement(
						sql("DELETE FROM ot_revisions WHERE id < ?"))) {
					ps.setInt(1, minId);
					ps.executeUpdate();
				}

				try (PreparedStatement ps = connection.prepareStatement(
						sql("DELETE FROM ot_diffs WHERE revision_id < ?"))) {
					ps.setInt(1, minId);
					ps.executeUpdate();
				}

				try (PreparedStatement ps = connection.prepareStatement(
						sql("DELETE FROM ot_merges WHERE max_parent_id < ?"))) {
					ps.setInt(1, minId);
					ps.executeUpdate();
				}

				connection.commit();
				logger.trace("Finish cleanup: {}", minId);
			}

			return null;
		});
	}

	// minimal db requests

	public CompletionStage<Revision> loadRevision(Integer revisionId) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(
						sql("SELECT type, timestamp, created_by FROM ot_revisions WHERE id=?"))) {
					ps.setInt(1, revisionId);
					ResultSet resultSet = ps.executeQuery();
					if (!resultSet.next()) throw new IllegalArgumentException();

					final String type = resultSet.getString(1);
					final String timestamp = resultSet.getString(2);
					final String createdBy = resultSet.getString(3);

					return new Revision(revisionId, type, timestamp, createdBy);
				}
			}
		});
	}

	public static class Revision {
		private final int id;
		private final String type;
		private final String timestamp;
		private final String createdBy;

		public Revision(int id, String type, String timestamp, String createdBy) {
			this.id = id;
			this.type = type;
			this.timestamp = timestamp;
			this.createdBy = createdBy;
		}

		public int getId() {
			return id;
		}

		public String getType() {
			return type;
		}

		public String getTimestamp() {
			return timestamp;
		}

		public String getCreatedBy() {
			return createdBy;
		}
	}

	public CompletionStage<List<Diff>> loadDiff(Integer revisionId) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement ps = connection.prepareStatement(
						sql("SELECT parent_id FROM ot_diffs WHERE revision_id=?"))) {
					ps.setInt(1, revisionId);
					final ResultSet resultSet = ps.executeQuery();

					final List<Diff> diffs = new ArrayList<>();
					while (resultSet.next()) {
						final int parentId = resultSet.getInt(1);
						diffs.add(new Diff(revisionId, parentId));
					}
					return diffs;
				}
			}
		});
	}

	public static class Diff {
		private final int revisionId;
		private final int parentId;

		public Diff(int revisionId, int parentId) {
			this.revisionId = revisionId;
			this.parentId = parentId;
		}

		public int getRevisionId() {
			return revisionId;
		}

		public int getParentId() {
			return parentId;
		}
	}

	public CompletionStage<List<Merge>> loadMerges(boolean loadDiff) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				final List<String> columns = asList("parent_ids", "min_parent_id", "max_parent_id", "timestamp", "created_by");
				if (loadDiff) columns.add("diff");

				final String selectedColumns = columns.stream().map(Object::toString).collect(joining(", "));
				try (PreparedStatement ps = connection.prepareStatement(
						sql("SELECT " + selectedColumns + " FROM ot_merges"))) {

					final ResultSet resultSet = ps.executeQuery();
					final List<Merge> list = new ArrayList<>();
					while (resultSet.next()) {
						final List<Integer> parentIds = Arrays.stream(resultSet.getString(1).split(" "))
								.map(String::trim)
								.filter(s -> !s.isEmpty())
								.map(Integer::parseInt)
								.collect(toList());
						final int minParentId = resultSet.getInt(2);
						final int maxParentId = resultSet.getInt(3);
						final Timestamp timestamp = Timestamp.valueOf(resultSet.getString(4));
						final String createdBy = resultSet.getString(5);
						final String diff = loadDiff ? resultSet.getString(6) : null;
						list.add(new Merge(parentIds, diff, minParentId, maxParentId, timestamp, createdBy));
					}
					return list;
				}
			}
		});
	}

	public static class Merge {
		private final List<Integer> parentIds;
		@Nullable
		private final String diff;
		private final int minParentId;
		private final int maxParentId;
		private final Timestamp timestamp;
		private final String createBy;

		public Merge(List<Integer> parentIds, @Nullable String diff, int minParentId, int maxParentId, Timestamp timestamp, String createBy) {
			this.parentIds = parentIds;
			this.diff = diff;
			this.minParentId = minParentId;
			this.maxParentId = maxParentId;
			this.timestamp = timestamp;
			this.createBy = createBy;
		}

		public List<Integer> getParentIds() {
			return parentIds;
		}

		@Nullable
		public String getDiff() {
			return diff;
		}

		public int getMinParentId() {
			return minParentId;
		}

		public int getMaxParentId() {
			return maxParentId;
		}

		public Timestamp getTimestamp() {
			return timestamp;
		}

		public String getCreateBy() {
			return createBy;
		}
	}

}
