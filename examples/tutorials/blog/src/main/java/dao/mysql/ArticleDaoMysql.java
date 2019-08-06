package dao.mysql;

import dao.ArticleDao;
import io.datakernel.async.Promise;
import io.datakernel.exception.StacklessException;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public final class ArticleDaoMysql implements ArticleDao {
	private final Executor executor;
	private final DataSource dataSource;

	public ArticleDaoMysql(Executor executor, DataSource dataSource) {
		this.executor = executor;
		this.dataSource = dataSource;
	}

	@Override
	public Promise<Long> addArticle(Article article) {
		return Promise.ofBlockingCallable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement stmt = connection.prepareStatement(
						"INSERT INTO article (title, text) VALUES (?, ?) ", RETURN_GENERATED_KEYS)) {
					stmt.setString(1, article.getTitle());
					stmt.setString(2, article.getText());
					stmt.executeUpdate();
					ResultSet keys = stmt.getGeneratedKeys();
					if (!keys.next()) {
						throw new StacklessException(ArticleDaoMysql.class, "Failed to insert");
					}
					return keys.getLong(1);
				}
			}
		});
	}

	@Override
	public Promise<@Nullable Article> getArticle(Long articleId) {
		return Promise.ofBlockingCallable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement stmt = connection.prepareStatement(
						"SELECT title, text FROM article WHERE id = ?")) {
					stmt.setLong(1, articleId);
					ResultSet resultSet = stmt.executeQuery();
					if (!resultSet.next()) {
						return null;
					}
					String title = resultSet.getString(1);
					String string = resultSet.getString(2);
					return new Article(title, string);
				}
			}
		});
	}

	@Override
	public Promise<Map<Long, Article>> getAllArticles() {
		return Promise.ofBlockingCallable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement stmt = connection.prepareStatement(
						"SELECT * FROM article ORDER BY id DESC")) {
					Map<Long, Article> result = new LinkedHashMap<>();
					ResultSet resultSet = stmt.executeQuery();
					while (resultSet.next()) {
						long id = resultSet.getLong(1);
						String title = resultSet.getString(2);
						String text = resultSet.getString(3);
						Article article = new Article(title, text);
						result.put(id, article);
					}
					return result;
				}
			}
		});
	}

	@Override
	public Promise<Void> updateArticle(Long articleId, String title, String text) {
		return Promise.ofBlockingRunnable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement stmt = connection.prepareStatement(
						"UPDATE article SET title=?, text=? WHERE id=?")) {
					stmt.setString(1, title);
					stmt.setString(2, text);
					stmt.setLong(3, articleId);

					stmt.executeUpdate();
				}
			}
		});
	}

	@Override
	public Promise<Void> deleteArticle(Long articleId) {
		return Promise.ofBlockingRunnable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement stmt = connection.prepareStatement(
						"DELETE FROM article WHERE id = ?")) {
					stmt.setLong(1, articleId);

					stmt.executeUpdate();
				}
			}
		});
	}
}
