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

package io.datakernel.test;

import ch.qos.logback.classic.Level;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class TestUtils {
	private static int activeStages = 0;

	private static byte[] loadResource(URL file) throws IOException {
		try (InputStream in = file.openStream()) {
			// reading file as resource
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = in.read(buffer)) != -1) {
				out.write(buffer, 0, size);
			}
			return out.toByteArray();
		}
	}

	public static byte[] loadResource(String name) {
		URL resource = Thread.currentThread().getContextClassLoader().getResource(name);
		if (resource == null) {
			throw new IllegalArgumentException(name);
		}
		try {
			return loadResource(resource);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static HikariDataSource dataSource(String databasePropertiesPath) throws IOException {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File(databasePropertiesPath))), StandardCharsets.UTF_8));
		HikariConfig configuration = new HikariConfig(properties);
		configuration.addDataSourceProperty("allowMultiQueries", true);
		return new HikariDataSource(configuration);
	}

	public static void executeScript(DataSource dataSource, Class<?> clazz) throws SQLException {
		executeScript(dataSource, clazz.getPackage().getName() + "/" + clazz.getSimpleName() + ".sql");
	}

	public static void executeScript(DataSource dataSource, String scriptName) throws SQLException {
		String sql = new String(loadResource(scriptName), Charset.forName("UTF-8"));
		execute(dataSource, sql);
	}

	private static void execute(DataSource dataSource, String sql) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql);
		}
	}

	public static void enableLogging(String name, Level level) {
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name);
		logger.setLevel(level);
	}

	public static void enableLogging(Level level) {
		enableLogging(Logger.ROOT_LOGGER_NAME, level);
	}

	public static void enableLogging(String name) {
		enableLogging(name, Level.TRACE);
	}

	public static void enableLogging() {
		enableLogging(Logger.ROOT_LOGGER_NAME, Level.TRACE);
	}

	public static <T> BiConsumer<T, Throwable> assertComplete(Consumer<T> consumer) {
		activeStages++;
		return (t, error) -> {
			activeStages--;
			if (error != null) {
				if (error.getClass() == AssertionError.class) {
					throw (AssertionError) error;
				}
				throw new AssertionError(error);
			}
			consumer.accept(t);
		};
	}

	public static <T> BiConsumer<T, Throwable> assertComplete() {
		return assertComplete($ -> {});
	}

	@SuppressWarnings("unchecked")
	public static <T, E extends Throwable> BiConsumer<T, Throwable> assertFailure(Class<E> errorClass, @Nullable String messagePattern, Consumer<E> consumer) {
		activeStages++;
		return (t, error) -> {
			activeStages--;
			if (error == null && errorClass == Throwable.class) {
				throw new AssertionError("Expected an error");
			}
			if (error == null || !errorClass.isAssignableFrom(error.getClass())) {
				throw new AssertionError("Expected an error of type " + errorClass.getName() + ", but got " + (error == null ? "none" : error.getClass().getSimpleName()));
			}
			if (messagePattern != null && !Pattern.compile(messagePattern).matcher(error.getMessage()).find()) {
				throw new AssertionError("Expected error message to match pattern `" + messagePattern + "`, but got message '" + error.getMessage() + "'");
			}
			consumer.accept((E) error);
		};
	}

	public static <T, E extends Throwable> BiConsumer<T, Throwable> assertFailure(Class<E> errorClass, Consumer<E> consumer) {
		return assertFailure(errorClass, null, consumer);
	}

	public static <T, E extends Throwable> BiConsumer<T, Throwable> assertFailure(Class<E> errorClass, String messagePattern) {
		return assertFailure(errorClass, messagePattern, $ -> {});
	}

	public static <T> BiConsumer<T, Throwable> assertFailure(Class<? extends Throwable> errorClass) {
		return assertFailure(errorClass, $ -> {});
	}

	public static <T> BiConsumer<T, Throwable> assertFailure(String messagePattern) {
		return assertFailure(Throwable.class, messagePattern);
	}

	public static <T> BiConsumer<T, Throwable> assertFailure(Consumer<Throwable> consumer) {
		return assertFailure(Throwable.class, consumer);
	}

	public static <T> BiConsumer<T, Throwable> assertFailure() {
		return assertFailure($ -> {});
	}

	public static int getActiveStages() {
		return activeStages;
	}

	public static void clearActiveStages() {
		activeStages = 0;
	}
}
