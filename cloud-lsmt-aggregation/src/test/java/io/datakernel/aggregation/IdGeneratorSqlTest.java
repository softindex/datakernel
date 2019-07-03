/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.aggregation;

import io.datakernel.aggregation.util.IdGeneratorSql;
import io.datakernel.aggregation.util.SqlAtomicSequence;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.ExecutorRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static io.datakernel.test.rules.ExecutorRule.getExecutor;
import static io.datakernel.util.SqlUtils.executeScript;
import static org.junit.Assert.assertEquals;

public class IdGeneratorSqlTest {
	private DataSource dataSource;
	private SqlAtomicSequence sequence;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final ExecutorRule executorRule = new ExecutorRule();

	@Before
	public void before() throws IOException, SQLException {
		dataSource = dataSource("test.properties");
		executeScript(dataSource, getClass());
		sequence = SqlAtomicSequence.ofLastInsertID("sequence", "next");
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
	public void testIdGeneratorSql() {
		IdGeneratorSql idGeneratorSql = IdGeneratorSql.create(Eventloop.getCurrentEventloop(), getExecutor(), dataSource, sequence);

		assertEquals(1, (long) await(idGeneratorSql.createId()));
		assertEquals(2, (long) await(idGeneratorSql.createId()));
		assertEquals(3, (long) await(idGeneratorSql.createId()));
	}

	@Test
	public void testIdGeneratorSql10() {
		IdGeneratorSql idGeneratorSql = IdGeneratorSql.create(Eventloop.getCurrentEventloop(), getExecutor(), dataSource, sequence)
				.withStride(10);
		for (int i = 1; i <= 25; i++) {
			assertEquals(i, (long) await(idGeneratorSql.createId()));
		}
		assertEquals(31, idGeneratorSql.getLimit());
	}

}
