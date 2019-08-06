package dao.ot.operation;

import dao.ArticleDao.Article;

import java.util.List;
import java.util.Map;

public interface ArticleOperation {

	Long getId();

	void apply(Map<Long, Article> articles);

	List<ArticleOperation> invert();

	boolean isEmpty();
}
