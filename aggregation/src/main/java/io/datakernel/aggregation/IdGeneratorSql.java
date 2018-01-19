package io.datakernel.aggregation;

import io.datakernel.async.AsyncCallable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;

public class IdGeneratorSql<K> implements IdGenerator<K>, EventloopJmxMBean {
	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final DataSource dataSource;

	private String insertSQL;
	private String extraSQL;

	private final StageStats stageCreateId = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

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

	public void withStatsSmoothingWindow(double smoothingWindowSeconds) {
		stageCreateId.setSmoothingWindow(smoothingWindowSeconds);
	}

	private final AsyncCallable<K> createId = AsyncCallable.of(this::doCreateId).with(stageCreateId::wrapper);

	@Override
	@SuppressWarnings("unchecked")
	public CompletionStage<K> createId() {
		return createId.call();
	}

	private CompletionStage<K> doCreateId() {
		return eventloop.callExecutor(executor, () -> {
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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public StageStats getStageCreateId() {
		return stageCreateId;
	}

	@JmxOperation
	public void resetStats() {
		stageCreateId.resetStats();
	}
}
