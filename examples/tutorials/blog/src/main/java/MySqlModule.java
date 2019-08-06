import com.mysql.cj.jdbc.MysqlDataSource;
import dao.ArticleDao;
import dao.mysql.ArticleDaoMysql;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executor;

public final class MySqlModule extends AbstractModule {
	private static final String DB_PROPERTIES_FILE = "db.properties";

	@Provides
	ArticleDao articleDaoMySql(Executor executor, DataSource dataSource) {
		return new ArticleDaoMysql(executor, dataSource);
	}

	@Provides
	DataSource dataSource() throws IOException, SQLException {
		Properties properties = new Properties();
		try (InputStream propsStream = ClassLoader.getSystemClassLoader().getResourceAsStream(DB_PROPERTIES_FILE)) {
			properties.load(propsStream);

			MysqlDataSource dataSource = new MysqlDataSource();
			dataSource.setServerName(properties.getProperty("dataSource.serverName"));
			dataSource.setDatabaseName(properties.getProperty("dataSource.databaseName"));
			dataSource.setUser(properties.getProperty("dataSource.user"));
			dataSource.setPassword(properties.getProperty("dataSource.password"));
			dataSource.setServerTimezone(properties.getProperty("dataSource.timeZone"));
			dataSource.setAllowMultiQueries(true);
			return dataSource;
		}
	}
}
