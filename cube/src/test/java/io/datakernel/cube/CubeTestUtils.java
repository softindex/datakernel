/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.cube;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.*;
import io.datakernel.serializer.SerializerBuilder;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;

import java.io.*;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Charsets.UTF_8;

public final class CubeTestUtils {
	public static LogToCubeMetadataStorage getLogToCubeMetadataStorage(Eventloop eventloop,
	                                                                   ExecutorService executor,
	                                                                   Configuration jooqConfiguration,
	                                                                   CubeMetadataStorageSql cubeMetadataStorageSql) {
		LogToCubeMetadataStorageSql metadataStorage = LogToCubeMetadataStorageSql.create(eventloop, executor,
				jooqConfiguration, cubeMetadataStorageSql);
		metadataStorage.truncateTables();
		return metadataStorage;
	}

	public static <T> LogManager<T> getLogManager(Class<T> logClass, Eventloop eventloop, ExecutorService executor,
	                                              DefiningClassLoader classLoader, Path logsDir) {
		return LogManagerImpl.create(eventloop,
				LocalFsLogFileSystem.create(eventloop, executor, logsDir),
				SerializerBuilder.create(classLoader).build(logClass));
	}

	public static Configuration getJooqConfiguration(String databasePropertiesPath, SQLDialect databaseDialect)
			throws IOException {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(
				new BufferedInputStream(new FileInputStream(
						new File(databasePropertiesPath))), UTF_8));
		HikariDataSource dataSource = new HikariDataSource(new HikariConfig(properties));

		Configuration jooqConfiguration = new DefaultConfiguration();
		jooqConfiguration.set(new DataSourceConnectionProvider(dataSource));
		jooqConfiguration.set(databaseDialect);

		return jooqConfiguration;
	}
}
