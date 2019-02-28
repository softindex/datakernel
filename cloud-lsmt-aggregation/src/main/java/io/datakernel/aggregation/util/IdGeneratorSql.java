package io.datakernel.aggregation.util;

import io.datakernel.aggregation.IdGenerator;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executor;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.Preconditions.checkState;

public final class IdGeneratorSql implements IdGenerator<Long>, EventloopJmxMBeanEx {

	private final Eventloop eventloop;
	private final Executor executor;
	private final DataSource dataSource;

	private final SqlAtomicSequence sequence;

	private int stride = 1;

	private long next;
	private long limit;

	private final PromiseStats promiseCreateId = PromiseStats.create(Duration.ofMinutes(5));

	private final AsyncSupplier<Void> reserveId = reuse(this::doReserveId).transformWith(promiseCreateId::wrapper);

	private IdGeneratorSql(Eventloop eventloop, Executor executor, DataSource dataSource, SqlAtomicSequence sequence) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.dataSource = dataSource;
		this.sequence = sequence;
	}

	public static IdGeneratorSql create(Eventloop eventloop, Executor executor, DataSource dataSource,
	                                    SqlAtomicSequence sequence) {
		return new IdGeneratorSql(eventloop, executor, dataSource, sequence);
	}

	public IdGeneratorSql withStride(int stride) {
		this.stride = stride;
		return this;
	}

	private Promise<Void> doReserveId() {
		int finalStride = stride;
		return Promise.ofBlockingCallable(executor, () -> getAndAdd(finalStride))
				.whenResult(id -> {
					next = id;
					limit = id + finalStride;
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
	public Promise<Long> createId() {
		checkState(next <= limit, "Cannot create id larger than the limit of " + limit);
		if (next < limit) {
			return Promise.of(next++);
		}
		return reserveId.get()
				.thenCompose($ -> createId());
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public PromiseStats getPromiseCreateId() {
		return promiseCreateId;
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
