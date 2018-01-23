package io.datakernel.aggregation;

import io.datakernel.aggregation.util.IdGeneratorSql;
import io.datakernel.aggregation.util.SqlAtomicSequence;
import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.dataSource;
import static io.datakernel.test.TestUtils.executeScript;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("WeakerAccess")
public class IdGeneratorSqlTest {
	DataSource dataSource;
	SqlAtomicSequence sequence;
	Eventloop eventloop;

	@Before
	public void before() throws IOException, SQLException {
		dataSource = dataSource("test.properties");
		executeScript(dataSource, this.getClass());
		sequence = SqlAtomicSequence.ofLastInsertID("sequence", "next");
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testSqlAtomicSequence() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			assertEquals(1, sequence.getAndAdd(connection, 1));
			assertEquals(2, sequence.getAndAdd(connection, 1));
			assertEquals(3, sequence.getAndAdd(connection, 1));
			assertEquals(4, sequence.getAndAdd(connection, 10));
			assertEquals(14, sequence.getAndAdd(connection, 10));
			assertEquals(24, sequence.getAndAdd(connection, 10));
			assertEquals(134, sequence.addAndGet(connection, 100));
			assertEquals(234, sequence.addAndGet(connection, 100));
		}
	}

	@Test
	public void testIdGeneratorSql() throws ExecutionException, InterruptedException {
		IdGeneratorSql idGeneratorSql = IdGeneratorSql.create(eventloop, newSingleThreadExecutor(), dataSource, sequence);

		CompletableFuture<Long> future1 = idGeneratorSql.createId().toCompletableFuture();
		eventloop.run();
		CompletableFuture<Long> future2 = idGeneratorSql.createId().toCompletableFuture();
		eventloop.run();
		CompletableFuture<Long> future3 = idGeneratorSql.createId().toCompletableFuture();
		eventloop.run();

		assertEquals(1, (long) future1.get());
		assertEquals(2, (long) future2.get());
		assertEquals(3, (long) future3.get());
	}

	@Test
	public void testIdGeneratorSql10() throws ExecutionException, InterruptedException {
		IdGeneratorSql idGeneratorSql = IdGeneratorSql.create(eventloop, newSingleThreadExecutor(), dataSource, sequence)
				.withStride(10);
		for (int i = 1; i <= 25; i++) {
			CompletableFuture<Long> future = idGeneratorSql.createId().toCompletableFuture();
			eventloop.run();
			assertEquals(i, (long) future.get());
		}
		assertEquals(31, idGeneratorSql.getLimit());
	}

}