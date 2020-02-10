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

package io.datakernel.common.sql;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static io.datakernel.common.Utils.loadResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlUtils {

	public static void executeScript(DataSource dataSource, Class<?> clazz) throws SQLException, IOException {
		executeScript(dataSource, clazz.getPackage().getName() + "/" + clazz.getSimpleName() + ".sql");
	}

	public static void executeScript(DataSource dataSource, String scriptName) throws SQLException, IOException {
		String sql = new String(loadResource(scriptName), UTF_8);
		execute(dataSource, sql);
	}

	public static void execute(DataSource dataSource, String sql) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql);
			}
		}
	}

}
