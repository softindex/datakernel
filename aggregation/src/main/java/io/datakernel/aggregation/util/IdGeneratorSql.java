package io.datakernel.aggregation.util;

import io.datakernel.aggregation.IdGenerator;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static io.datakernel.util.Preconditions.checkState;

public final class IdGeneratorSql implements IdGenerator<Long>, EventloopJmxMBeanEx {

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final DataSource dataSource;

	private final SqlAtomicSequence sequence;

	private int stride = 1;

	private long next;
	private long limit;

	private final StageStats stageCreateId = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);

	private final AsyncCallable<Void> reserveId = sharedCall(this::doReserveId).with(stageCreateId::wrapper);

	private IdGeneratorSql(Eventloop eventloop, ExecutorService executor, DataSource dataSource, SqlAtomicSequence sequence) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.dataSource = dataSource;
		this.sequence = sequence;
	}

	public static IdGeneratorSql create(Eventloop eventloop, ExecutorService executor, DataSource dataSource,
	                                    SqlAtomicSequence sequence) {
		return new IdGeneratorSql(eventloop, executor, dataSource, sequence);
	}

	public IdGeneratorSql withStride(int stride) {
		this.stride = stride;
		return this;
	}

	private Stage<Void> doReserveId() {
		int finalStride = stride;
		return Stage.ofCallable(executor, () -> getAndAdd(finalStride))
				.thenAccept(next -> {
					this.next = next;
					this.limit = next + finalStride;
				})
				.toVoid();
	}

	private long getAndAdd(int stride) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(true);
			return sequence.getAndAdd(connection, stride);
		}
	}

	@Override
	public Stage<Long> createId() {
		checkState(next <= limit);
		if (next < limit) {
			return Stage.of(next++);
		}
		return reserveId.call()
				.thenCompose($ -> createId());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public StageStats getStageCreateId() {
		return stageCreateId;
	}

	@JmxAttribute
	public int getStride() {
		return stride;
	}

	@JmxAttribute
	public void setStride(int stride) {
		this.stride = stride;
	}

	@JmxAttribute
	public long getNext() {
		return next;
	}

	@JmxAttribute
	public long getLimit() {
		return limit;
	}
}
