package io.datakernel.ot;

import com.google.common.base.Joiner;
import com.google.gson.TypeAdapter;
import io.datakernel.utils.GsonAdapters;
import io.datakernel.utils.JsonException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static io.datakernel.utils.GsonAdapters.indent;
import static io.datakernel.utils.GsonAdapters.ofList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

public class OTRemoteSql<D> implements OTRemoteBlocking<Integer, D> {
	public static final String TABLE_REVISION = "ot_revision";
	public static final String TABLE_DIFFS = "ot_diffs";

	private final DataSource dataSource;
	private final TypeAdapter<List<D>> diffsAdapter;

	private final String tableRevision;
	private final String tableDiffs;
	private final String scope;
	private final String createdBy;

	private OTRemoteSql(TypeAdapter<List<D>> diffsAdapter, DataSource dataSource, String tableRevision, String tableDiffs, String scope, String createdBy) {
		this.dataSource = dataSource;
		this.diffsAdapter = diffsAdapter;
		this.tableRevision = tableRevision;
		this.tableDiffs = tableDiffs;
		this.scope = scope;
		this.createdBy = createdBy;
	}

	public static <D> OTRemoteSql<D> create(DataSource dataSource, TypeAdapter<D> diffAdapter) {
		return new OTRemoteSql<>(indent(ofList(diffAdapter), "\t"), dataSource, TABLE_REVISION, TABLE_DIFFS, null, null);
	}

	public OTRemoteSql<D> withScope(String scope) {
		return new OTRemoteSql<>(diffsAdapter, dataSource, tableRevision, tableDiffs, scope, createdBy);
	}

	public OTRemoteSql<D> withCreatedBy(String createdBy) {
		return new OTRemoteSql<>(diffsAdapter, dataSource, tableRevision, tableDiffs, scope, createdBy);
	}

	public OTRemoteSql<D> withTables(String tableRevision, String tableDiffs) {
		return new OTRemoteSql<>(diffsAdapter, dataSource, tableRevision, tableDiffs, scope, createdBy);
	}

	private String sql(String sql) {
		if (scope == null) {
			sql = sql.replace("scope=?", "NULL IS ?");
		}
		return sql
				.replace(TABLE_REVISION, tableRevision)
				.replace(TABLE_DIFFS, tableDiffs);
	}

	public void truncateTables() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql("TRUNCATE TABLE ot_diffs"));
			statement.execute(sql("TRUNCATE TABLE ot_revisions"));
		}
	}

	@Override
	public Integer createId() throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(true);
			try (PreparedStatement statement = connection.prepareStatement(
					sql("INSERT INTO ot_revisions(scope, type, created_by) VALUES (?, ?, ?)"),
					Statement.RETURN_GENERATED_KEYS)) {
				statement.setString(1, scope);
				statement.setString(2, "NEW");
				statement.setString(3, createdBy);
				statement.executeUpdate();
				ResultSet generatedKeys = statement.getGeneratedKeys();
				generatedKeys.next();
				return generatedKeys.getInt(1);
			}
		}
	}

	private String toJson(List<D> diffs) throws JsonException {
		return GsonAdapters.toJson(diffsAdapter, diffs);
	}

	@SuppressWarnings("unchecked")
	private List<D> fromJson(String json) throws JsonException {
		return GsonAdapters.fromJson(diffsAdapter, json);
	}

	public void push(OTCommit<Integer, D> commit) throws Exception {
		push(singletonList(commit));
	}

	@Override
	public void push(List<OTCommit<Integer, D>> commits) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);

			for (int i = 0; i < commits.size(); i++) {
				OTCommit<Integer, D> commit = commits.get(i);
				try (PreparedStatement ps = connection.prepareStatement(
						sql("REPLACE INTO ot_revisions (scope, id, type, checkpoint) VALUES (?, ?, ?, ?)"))) {
					ps.setString(1, scope);
					ps.setInt(2, commit.getId());
					ps.setString(3, (i == commits.size() - 1) ? "HEAD" : "INNER");
					ps.setString(4, commit.getCheckpoint() == null ? null : toJson(commit.getCheckpoint()));
					ps.executeUpdate();
				}

				for (Map.Entry<Integer, List<D>> entry : commit.getParents().entrySet()) {
					Integer parentId = entry.getKey();
					List<D> diff = entry.getValue();

					try (PreparedStatement ps = connection.prepareStatement(
							sql("INSERT INTO ot_diffs(scope, revision_id, parent_id, diff) VALUES (?, ?, ?, ?)"))) {
						ps.setString(1, scope);
						ps.setInt(2, commit.getId());
						ps.setInt(3, parentId);
						ps.setString(4, toJson(diff));
						ps.executeUpdate();
					}
				}
			}

			Set<Integer> parents = commits.get(0).getParents().keySet();

			if (!parents.isEmpty()) {
				try (PreparedStatement ps = connection.prepareStatement(
						sql("UPDATE ot_revisions SET type='INNER' WHERE scope=?"
								+ " AND id IN (" + Joiner.on(",").join(nCopies(parents.size(), "?")) + ")"))) {
					ps.setString(1, scope);
					int pos = 2;
					for (Integer parent : parents) {
						ps.setInt(pos++, parent);
					}
					ps.executeUpdate();
				}
			}

			connection.commit();
		}
	}

	@Override
	public Set<Integer> getHeads() throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement ps = connection.prepareStatement(
					sql("SELECT id FROM ot_revisions WHERE scope=? AND type='HEAD'"))) {
				ps.setString(1, scope);
				ResultSet resultSet = ps.executeQuery();
				Set<Integer> result = new HashSet<>();
				while (resultSet.next()) {
					int id = resultSet.getInt(1);
					result.add(id);
				}
				return result;
			}
		}
	}

	@Override
	public Integer getCheckpoint() throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement ps = connection.prepareStatement(
					sql("SELECT MAX(id) FROM ot_revisions WHERE scope=? AND checkpoint IS NOT NULL"))) {
				ps.setString(1, scope);
				ResultSet resultSet = ps.executeQuery();
				if (resultSet.next()) {
					return resultSet.getInt(1);
				}
				throw new IllegalStateException("Could not find checkpoint");
			}
		}
	}

	@Override
	public OTCommit<Integer, D> loadCommit(Integer revisionId) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			List<D> checkpoint = null;
			try (PreparedStatement ps = connection.prepareStatement(
					sql("SELECT checkpoint FROM ot_revisions WHERE scope=? AND id=?"))) {
				ps.setString(1, scope);
				ps.setInt(2, revisionId);
				ResultSet resultSet = ps.executeQuery();
				if (!resultSet.next())
					throw new IllegalArgumentException();
				String checkpointString = resultSet.getString(1);
				if (checkpointString != null)
					checkpoint = fromJson(checkpointString);
			}

			Map<Integer, List<D>> parentDiffs = new HashMap<>();

			try (PreparedStatement ps = connection.prepareStatement(
					sql("SELECT parent_id, diff FROM ot_diffs WHERE scope=? AND revision_id=?"))) {
				ps.setString(1, scope);
				ps.setInt(2, revisionId);
				ResultSet resultSet = ps.executeQuery();
				while (resultSet.next()) {
					int parentId = resultSet.getInt(1);
					String diffString = resultSet.getString(2);
					List<D> diff = fromJson(diffString);
					parentDiffs.put(parentId, diff);
				}
			}

			return OTCommit.of(revisionId, checkpoint, parentDiffs);
		}
	}

}
