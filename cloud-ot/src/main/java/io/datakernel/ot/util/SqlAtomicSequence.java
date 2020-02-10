package io.datakernel.ot.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface SqlAtomicSequence {
	default long getAndAdd(Connection connection, int stride) throws SQLException {
		return addAndGet(connection, stride) - stride;
	}

	default long addAndGet(Connection connection, int stride) throws SQLException {
		return getAndAdd(connection, stride) + stride;
	}

	static SqlAtomicSequence ofLastInsertID(String table, String field) {
		return ofLastInsertID(table, field, null);
	}

	static SqlAtomicSequence ofLastInsertID(String table, String field, String where) {
		String sql = "UPDATE {table} SET {field} = LAST_INSERT_ID({table}.{field}) + :stride"
				.replace("{table}", "`" + table + "`")
				.replace("{field}", "`" + field + "`");
		if (where != null) {
			sql += " WHERE " + where;
		}
		String finalSql = sql;
		return new SqlAtomicSequence() {
			@Override
			public long getAndAdd(Connection connection, int stride) throws SQLException {
				try (Statement statement = connection.createStatement()) {
					statement.execute(finalSql.replace(":stride", Integer.toString(stride)), Statement.RETURN_GENERATED_KEYS);
					ResultSet generatedKeys = statement.getGeneratedKeys();
					generatedKeys.next();
					return generatedKeys.getLong(1);
				}
			}
		};
	}
}
