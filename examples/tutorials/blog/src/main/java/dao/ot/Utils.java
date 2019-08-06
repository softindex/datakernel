package dao.ot;

import dao.ArticleDao.Article;
import dao.ot.operation.AddOrDeleteArticle;
import dao.ot.operation.ArticleOperation;
import dao.ot.operation.EditArticle;
import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;

import java.util.Comparator;

import static dao.ot.operation.AddOrDeleteArticle.add;
import static dao.ot.operation.AddOrDeleteArticle.delete;
import static io.datakernel.codec.StructuredCodecs.*;

public final class Utils {

	public static final StructuredCodec<ArticleOperation> ARTICLE_OPERATION_CODEC = CodecSubtype.<ArticleOperation>create()
			.with(AddOrDeleteArticle.class,
					tuple(AddOrDeleteArticle::of,
							AddOrDeleteArticle::getId, LONG_CODEC,
							op -> op.getArticle().getTitle(), STRING_CODEC,
							op -> op.getArticle().getText(), STRING_CODEC,
							AddOrDeleteArticle::isRemove, BOOLEAN_CODEC))
			.with(EditArticle.class,
					tuple(EditArticle::new,
							EditArticle::getId, LONG_CODEC,
							EditArticle::getPrevTitle, STRING_CODEC,
							EditArticle::getNextTitle, STRING_CODEC,
							EditArticle::getPrevText, STRING_CODEC,
							EditArticle::getNextText, STRING_CODEC));

	public static OTSystem<ArticleOperation> createOTSystem() {
		return OTSystemImpl.<ArticleOperation>create()
				.withEmptyPredicate(AddOrDeleteArticle.class, ArticleOperation::isEmpty)
				.withEmptyPredicate(EditArticle.class, ArticleOperation::isEmpty)

				.withInvertFunction(AddOrDeleteArticle.class, ArticleOperation::invert)
				.withInvertFunction(EditArticle.class, ArticleOperation::invert)

				.withTransformFunction(AddOrDeleteArticle.class, AddOrDeleteArticle.class, (left, right) -> {
					if (left.getId().equals(right.getId())) {
						// only 2 removes are possible here

						if (!(left.isRemove() && right.isRemove())) {
							throw new OTTransformException("ID collision");
						}

						if (ARTICLE_CUSTOM_COMPARATOR.compare(left.getArticle(), right.getArticle()) != 0) {
							throw new OTTransformException("Same ID, different state");
						}
						return TransformResult.empty();
					}

					return TransformResult.of(right, left);
				})
				.withTransformFunction(EditArticle.class, EditArticle.class, (left, right) -> {
					if (left.getId().equals(right.getId())) {
						if (!(left.getPrevTitle().equals(right.getPrevTitle()) && left.getPrevText().equals(right.getPrevText()))) {
							throw new OTTransformException("Corrupted state");
						}

						int compare = EDIT_ARTICLE_CUSTOM_COMPARATOR.compare(left, right);
						if (compare > 0) {
							return TransformResult.right(
									new EditArticle(
											left.getId(),
											right.getNextTitle(),
											right.getNextText(),
											left.getNextTitle(),
											left.getPrevText())
							);
						} else if (compare < 0) {
							return TransformResult.left(
									new EditArticle(
											left.getId(),
											left.getNextTitle(),
											left.getNextText(),
											right.getNextTitle(),
											right.getPrevText())
							);
						} else {
							return TransformResult.empty();
						}
					}

					return TransformResult.of(right, left);
				})
				.withTransformFunction(AddOrDeleteArticle.class, EditArticle.class, (left, right) -> {
					if (left.getId().equals(right.getId())) {
						if (left.isRemove()) {
							return TransformResult.right(delete(left.getId(), left.getArticle()));
						} else {
							throw new OTTransformException("ID collision");
						}
					}

					return TransformResult.of(right, left);
				})

				.withSquashFunction(AddOrDeleteArticle.class, AddOrDeleteArticle.class, (first, second) -> {
					if (first.getId().equals(second.getId())) {
						// add and delete right away
						if (!first.isRemove() && second.isRemove()) {
							return AddOrDeleteArticle.EMPTY;
						}
					}
					return null;
				})
				.withSquashFunction(EditArticle.class, EditArticle.class, (first, second) -> {
					if (first.getId().equals(second.getId())) {
						return new EditArticle(
								first.getId(),
								first.getPrevTitle(),
								first.getPrevText(),
								second.getNextTitle(),
								second.getNextText()
						);
					}
					return null;

				})
				.withSquashFunction(AddOrDeleteArticle.class, EditArticle.class, (first, second) -> {
					if (first.getId().equals(second.getId())) {
						if (!first.isRemove()) {
							return add(first.getId(), new Article(second.getNextTitle(), second.getNextText()));
						}
					}

					return null;
				})
				.withSquashFunction(EditArticle.class, AddOrDeleteArticle.class, (first, second) -> {
					if (first.getId().equals(second.getId())) {
						return second;
					}

					return null;
				});
	}

	private static final Comparator<Article> ARTICLE_CUSTOM_COMPARATOR = Comparator.comparing(Article::getTitle)
			.thenComparing(Article::getText);

	private static final Comparator<EditArticle> EDIT_ARTICLE_CUSTOM_COMPARATOR = Comparator.comparing(EditArticle::getNextTitle)
			.thenComparing(EditArticle::getNextText);
}
