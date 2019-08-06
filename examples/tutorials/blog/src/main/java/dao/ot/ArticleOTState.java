package dao.ot;

import dao.ArticleDao.Article;
import dao.ot.operation.ArticleOperation;
import io.datakernel.ot.OTState;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Comparator.reverseOrder;

public final class ArticleOTState implements OTState<ArticleOperation> {
	private final Map<Long, Article> articles = new TreeMap<>(reverseOrder());

	@Override
	public void init() {
		articles.clear();
	}

	@Override
	public void apply(ArticleOperation op) {
		op.apply(articles);
	}

	public Map<Long, Article> getArticles() {
		return Collections.unmodifiableMap(articles);
	}
}
