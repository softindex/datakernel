package io.datakernel.test;

import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

import static io.datakernel.test.TestUtils.dataSource;

public class TestUtilsTest {
	@Test
	public void execute() throws IOException, SQLException {
		DataSource dataSource = dataSource("test.properties");
		TestUtils.executeScript(dataSource, "io.datakernel.test/script.sql");
	}
}