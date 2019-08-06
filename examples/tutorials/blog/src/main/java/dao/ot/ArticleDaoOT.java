package dao.ot;

import dao.ArticleDao;
import dao.ot.operation.ArticleOperation;
import dao.ot.operation.EditArticle;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.ot.OTStateManager;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static dao.ot.operation.AddOrDeleteArticle.add;
import static dao.ot.operation.AddOrDeleteArticle.delete;
import static java.util.Collections.unmodifiableMap;

public final class ArticleDaoOT implements ArticleDao {
	private final OTStateManager<CommitId, ArticleOperation> stateManager;
	private final AsyncSupplier<Long> idGenerator;
	private final Map<Long, Article> stateView;

	public ArticleDaoOT(OTStateManager<CommitId, ArticleOperation> stateManager, AsyncSupplier<Long> idGenerator) {
		this.stateManager = stateManager;
		this.idGenerator = idGenerator;
		this.stateView = unmodifiableMap(((ArticleOTState) stateManager.getState()).getArticles());
	}

	@Override
	public Promise<Long> addArticle(Article article) {
		return idGenerator.get()
				.then(id -> {
					ArticleOperation op = add(id, article);
					stateManager.add(op);
					return stateManager.sync()
							.map($ -> id);
				});
	}

	@Override
	public Promise<@Nullable Article> getArticle(Long articleId) {
		return Promise.of(stateView.get(articleId));
	}

	@Override
	public Promise<Map<Long, Article>> getAllArticles() {
		return Promise.of(stateView);
	}

	@Override
	public Promise<Void> updateArticle(Long articleId, String title, String text) {
		Article article = stateView.get(articleId);

		if (article == null) {
			return Promise.of(null);
		}

		ArticleOperation op = new EditArticle(articleId, article.getTitle(), title, article.getText(), text);
		stateManager.add(op);
		return stateManager.sync();
	}

	@Override
	public Promise<Void> deleteArticle(Long articleId) {
		Article article = stateView.get(articleId);

		if (article == null) {
			return Promise.of(null);
		}

		ArticleOperation op = delete(articleId, article);
		stateManager.add(op);
		return stateManager.sync();
	}

}
