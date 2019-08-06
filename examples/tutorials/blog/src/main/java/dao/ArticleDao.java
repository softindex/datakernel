package dao;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public interface ArticleDao {
	Promise<Long> addArticle(Article article);

	Promise<@Nullable Article> getArticle(Long articleId);

	Promise<Map<Long, Article>> getAllArticles();

	Promise<Void> updateArticle(Long articleId, String title, String text);

	Promise<Void> deleteArticle(Long articleId);

	final class Article {
		private String title;
		private String text;

		public Article(String title, String text) {
			this.title = title;
			this.text = text;
		}

		public String getTitle() {
			return title;
		}

		public String getText() {
			return text;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setText(String text) {
			this.text = text;
		}
	}
}
