package dao.ot.operation;

import dao.ArticleDao.Article;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public final class EditArticle implements ArticleOperation {
	private final Long id;

	private final String prevTitle;
	private final String nextTitle;

	private final String prevText;
	private final String nextText;

	public EditArticle(Long id, String prevTitle, String nextTitle, String prevText, String nextText) {
		this.id = id;
		this.prevTitle = prevTitle;
		this.nextTitle = nextTitle;
		this.prevText = prevText;
		this.nextText = nextText;
	}

	@Override
	public void apply(Map<Long, Article> articles) {
		Article previous = articles.get(id);
		if (previous != null) {
			previous.setTitle(nextTitle);
			previous.setText(nextText);
		} else {
			throw new IllegalStateException("Invalid operation");
		}
	}

	@Override
	public List<ArticleOperation> invert() {
		return singletonList(new EditArticle(id, nextTitle, prevTitle, nextText, prevText));
	}

	@Override
	public boolean isEmpty() {
		return prevTitle.equals(nextTitle) && prevText.equals(nextText);
	}

	@Override
	public Long getId() {
		return id;
	}

	public String getPrevTitle() {
		return prevTitle;
	}

	public String getNextTitle() {
		return nextTitle;
	}

	public String getPrevText() {
		return prevText;
	}

	public String getNextText() {
		return nextText;
	}
}
