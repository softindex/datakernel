package io.datakernel.aggregation;

import io.datakernel.eventloop.Eventloop;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public class IdGeneratorSql<K> implements IdGenerator<K> {
	private final ExecutorService executor;
	private final DataSource dataSource;

	private String insertSQL;
	private String extraSQL;

	private IdGeneratorSql(ExecutorService executor, DataSource dataSource) {
		this.executor = executor;
		this.dataSource = dataSource;
	}

	public static <K> IdGeneratorSql<K> create(Eventloop eventloop, ExecutorService executor, DataSource dataSource) {
		return new IdGeneratorSql<K>(executor, dataSource);
	}

	public IdGeneratorSql<K> withInsertSQL(String insertSQL) {
		this.insertSQL = insertSQL;
		return this;
	}

	public IdGeneratorSql<K> withExtraSQL(String extraSQL) {
		this.extraSQL = extraSQL;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CompletionStage<K> createId() {
		return getCurrentEventloop().callConcurrently(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(true);
				K result;
				try (Statement statement = connection.createStatement()) {
					statement.execute(insertSQL, Statement.RETURN_GENERATED_KEYS);
					ResultSet generatedKeys = statement.getGeneratedKeys();
					generatedKeys.next();
					result = (K) generatedKeys.getObject(1);
					if (extraSQL != null) {
						statement.executeUpdate(extraSQL);
					}
				}
				return result;
			}
		});
	}

}
