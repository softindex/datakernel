package dao.ot.operation;

import dao.ArticleDao.Article;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public final class AddOrDeleteArticle implements ArticleOperation {
	public static final AddOrDeleteArticle EMPTY = new AddOrDeleteArticle(0L, new Article("", ""), true);

	private final Long id;
	private final Article article;
	private final boolean remove;

	private AddOrDeleteArticle(Long id, Article article, boolean remove) {
		this.id = id;
		this.article = article;
		this.remove = remove;
	}

	public static AddOrDeleteArticle of(Long id, String title, String text, boolean remove) {
		return new AddOrDeleteArticle(id, new Article(title, text), remove);
	}

	public static AddOrDeleteArticle add(Long id, Article article) {
		return new AddOrDeleteArticle(id, article, false);
	}

	public static AddOrDeleteArticle add(Long id, String title, String text) {
		return add(id, new Article(title, text));
	}

	public static AddOrDeleteArticle delete(Long id, Article article) {
		return new AddOrDeleteArticle(id, article, true);
	}

	public static AddOrDeleteArticle delete(Long id, String title, String text) {
		return delete(id, new Article(title, text));
	}

	@Override
	public void apply(Map<Long, Article> articles) {
		boolean isValidOp;
		if (remove) {
			Article removed = articles.remove(id);
			isValidOp = removed != null;
		} else {
			Article previous = articles.put(id, article);
			isValidOp = previous == null;
		}

		if (!isValidOp) {
			throw new IllegalStateException("Invalid operation");
		}
	}

	@Override
	public List<ArticleOperation> invert() {
		return singletonList(new AddOrDeleteArticle(id, article, !remove));
	}

	@Override
	public boolean isEmpty() {
		return id == 0;
	}

	@Override
	public Long getId() {
		return id;
	}

	public Article getArticle() {
		return article;
	}

	public boolean isRemove() {
		return remove;
	}
}
