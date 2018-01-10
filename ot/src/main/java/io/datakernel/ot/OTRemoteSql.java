package io.datakernel.ot;

import com.google.gson.TypeAdapter;
import io.datakernel.utils.GsonAdapters;
import io.datakernel.utils.JsonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.utils.GsonAdapters.indent;
import static io.datakernel.utils.GsonAdapters.ofList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.*;

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
		TypeAdapter<List<D>> listAdapter = indent(ofList(diffAdapter), "\t");

		TypeAdapter<Map<Integer, List<D>>> mapDiffsAdapter = GsonAdapters.transform(GsonAdapters.ofMap(listAdapter),
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
					int id = generatedKeys.getInt(1);
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

	private static String in(int n) {
		return nCopies(n, "?").stream().collect(joining(", ", "(", ")"));
	}

	public CompletionStage<Void> push(OTCommit<Integer, D> commit) {
		return push(singletonList(commit));
	}

	@Override
	public CompletionStage<Void> push(Collection<OTCommit<Integer, D>> commits) {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			logger.trace("Push {} commits: {}", commits.size(),
					commits.stream().map(OTCommit::idsToString).collect(toList()));

			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(false);

				for (OTCommit<Integer, D> commit : commits) {
					for (Integer parentId : commit.getParents().keySet()) {
						List<D> diff = commit.getParents().get(parentId);
						try (PreparedStatement ps = connection.prepareStatement(
								sql("INSERT INTO ot_diffs(revision_id, parent_id, diff) VALUES (?, ?, ?)"))) {
							ps.setInt(1, commit.getId());
							ps.setInt(2, parentId);
							ps.setString(3, toJson(diff));
							ps.executeUpdate();
						}
					}
				}

				Set<Integer> commitIds = commits.stream().map(OTCommit::getId).collect(toSet());
				Set<Integer> commitsParentIds = commits.stream().flatMap(commit -> commit.getParents().keySet().stream()).collect(toSet());
				Set<Integer> headCommitIds = difference(commitIds, commitsParentIds);
				Set<Integer> innerCommitIds = union(commitsParentIds, difference(commitIds, headCommitIds));

				if (!headCommitIds.isEmpty()) {
					try (PreparedStatement ps = connection.prepareStatement(
							sql("UPDATE ot_revisions SET type='HEAD' WHERE type='NEW' AND id IN " + in(headCommitIds.size())))) {
						int pos = 1;
						for (Integer id : headCommitIds) {
							ps.setInt(pos++, id);
						}
						ps.executeUpdate();
					}
				}

				if (!innerCommitIds.isEmpty()) {
					try (PreparedStatement ps = connection.prepareStatement(
							sql("UPDATE ot_revisions SET type='INNER' WHERE id IN " + in(innerCommitIds.size())))) {
						int pos = 1;
						for (Integer id : innerCommitIds) {
							ps.setInt(pos++, id);
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
						"AND snapshot IS NOT NULL "))) {
					ps.setInt(1, revisionId);
					ResultSet resultSet = ps.executeQuery();

					if (!resultSet.next()) throw new IOException("No snapshot for id: " + revisionId);

					List<D> snapshot = fromJson(resultSet.getString(1));
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
						"AND snapshot IS NOT NULL"))) {
					ps.setInt(1, revisionId);
					ResultSet resultSet = ps.executeQuery();

					resultSet.next();
					boolean result = resultSet.getInt(1) == 1;
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
				Map<Integer, List<D>> parentDiffs = new HashMap<>();

				try (PreparedStatement ps = connection.prepareStatement(
						sql("SELECT parent_id, diff FROM ot_diffs WHERE revision_id = ?"))) {
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
							sql("SELECT COUNT(*) FROM ot_revisions WHERE id = ?"))) {
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
			try (Connection connection = dataSource.getConnection()) {
				String snapshot = toJson(otSystem.squash(diffs));
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

				connection.commit();
				logger.trace("Finish cleanup: {}", minId);
			}

			return null;
		});
	}

}
