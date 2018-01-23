package io.datakernel.test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestUtils {

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

}
