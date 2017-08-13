package io.datakernel.aggregation;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class IdGeneratorSql<K> implements IdGenerator<K> {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final DataSource dataSource;

	private String insertSQL;
	private String extraSQL;

	private IdGeneratorSql(Eventloop eventloop, ExecutorService executor, DataSource dataSource) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.dataSource = dataSource;
	}

	public static <K> IdGeneratorSql<K> create(Eventloop eventloop, ExecutorService executor, DataSource dataSource) {
		return new IdGeneratorSql<K>(eventloop, executor, dataSource);
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
	public void createId(ResultCallback<K> callback) {
		eventloop.callConcurrently(executor, new Callable<K>() {
			@Override
			public K call() throws Exception {
				return createId();
			}
		}, callback);
	}

	@SuppressWarnings("unchecked")
	public K createId() throws Exception {
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
			connection.commit();
			return result;
		}
	}

}
