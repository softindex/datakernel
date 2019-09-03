package io.global.forum.ot.post;

import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.forum.ot.post.operation.*;
import io.global.ot.map.SetValue;
import io.global.ot.map.SetValueOTSystem;
import io.global.ot.name.ChangeName;
import io.global.ot.name.NameOTSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.ot.TransformResult.empty;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public final class ThreadOTSystem {

	private ThreadOTSystem() {
		throw new AssertionError();
	}

	public static final OTSystem<ThreadOperation> SYSTEM = create();

	private static OTSystem<ThreadOperation> create() {
		OTSystem<PostChangesOperation> postChangesOTSystem = postChangesOTSystem();
		return OTSystemImpl.<ThreadOperation>create()
				.withEmptyPredicate(AddPost.class, op -> op.getInitialTimestamp() == -1)
				.withEmptyPredicate(PostChangesOperation.class, postChangesOTSystem::isEmpty)

				.withInvertFunction(AddPost.class, op -> singletonList(op.invert()))
				.withInvertFunction(PostChangesOperation.class, op -> postChangesOTSystem.invert(singletonList(op)))

				.withSquashFunction(PostChangesOperation.class, PostChangesOperation.class, (first, second) -> {
					List<PostChangesOperation> squashed = postChangesOTSystem.squash(asList(first, second));
					return squashed.isEmpty() ? PostChangesOperation.EMPTY : first(squashed);
				})
				.withSquashFunction(AddPost.class, AddPost.class, (first, second) -> {
					if (first.isInversion(second)) {
						return AddPost.EMPTY;
					}
					return null;
				})

				.withTransformFunction(AddPost.class, AddPost.class, (left, right) -> {
					if (left.getPostId().equals(right.getPostId())) {
						throw new OTTransformException("ID collision");
					}
					if (left.getParentId() == null || right.getParentId() == null) {
						throw new OTTransformException("Multiple root posts");
					}
					return TransformResult.of(right, left);
				})
				.withTransformFunction(PostChangesOperation.class, PostChangesOperation.class, postChangesOTSystem::transform)
				.withTransformFunction(AddPost.class, PostChangesOperation.class, (left, right) -> {
					String postId = left.getPostId();
					PostChangesOperation changesById = extractOpsById(postId, right);
					if (changesById.isEmpty()) {
						return TransformResult.of(right, left);
					}
					if (!left.isRemove()) {
						throw new OTTransformException("ID collision");
					}
					// remove wins
					List<ThreadOperation> rightTransformed = new ArrayList<>(postChangesOTSystem.invert(singletonList(changesById)));
					rightTransformed.add(left);
					return TransformResult.right(rightTransformed);
				});
	}

	private static OTSystem<PostChangesOperation> postChangesOTSystem() {
		return MergedOTSystem.mergeOtSystems(PostChangesOperation::new,
				PostChangesOperation::getChangeContentOps, contentOTSystem(),
				PostChangesOperation::getChangeAttachmentsOps, attachmentsOTSystem(),
				PostChangesOperation::getChangeRatingOps, ratingOTSystem(),
				PostChangesOperation::getDeletePostOps, deleteOTSystem(),
				PostChangesOperation::getChangeLastEditTimestamps, lastEditTimestampOTSystem());
	}

	private static OTSystem<ChangeLastEditTimestamp> lastEditTimestampOTSystem() {
		return OTSystemImpl.<ChangeLastEditTimestamp>create()
				.withEmptyPredicate(ChangeLastEditTimestamp.class, op -> op.getNextTimestamp() == op.getPrevTimestamp())
				.withInvertFunction(ChangeLastEditTimestamp.class, op ->
						singletonList(new ChangeLastEditTimestamp(op.getPostId(), op.getNextTimestamp(), op.getPrevTimestamp())))
				.withSquashFunction(ChangeLastEditTimestamp.class, ChangeLastEditTimestamp.class, (first, second) -> {
					if (!first.getPostId().equals(second.getPostId())) {
						return null;
					}
					return new ChangeLastEditTimestamp(first.getPostId(), first.getPrevTimestamp(), second.getNextTimestamp());
				})
				.withTransformFunction(ChangeLastEditTimestamp.class, ChangeLastEditTimestamp.class, (left, right) -> {
					if (!left.getPostId().equals(right.getPostId())) {
						return TransformResult.of(right, left);
					}
					if (left.getPrevTimestamp() != right.getPrevTimestamp()) {
						throw new OTTransformException("Previous timestamps should be equals");
					}
					if (left.getNextTimestamp() > right.getNextTimestamp()) {
						return TransformResult.right(new ChangeLastEditTimestamp(left.getPostId(), right.getNextTimestamp(), left.getNextTimestamp()));
					} else if (left.getNextTimestamp() < right.getNextTimestamp()) {
						return TransformResult.left(new ChangeLastEditTimestamp(left.getPostId(), left.getNextTimestamp(), right.getNextTimestamp()));
					}
					return empty();
				});
	}

	private static OTSystem<DeletePost> deleteOTSystem() {
		return OTSystemImpl.<DeletePost>create()
				.withEmptyPredicate(DeletePost.class, op -> op.getTimestamp() == -1)
				.withInvertFunction(DeletePost.class, op -> singletonList(op.invert()))
				.withSquashFunction(DeletePost.class, DeletePost.class, (first, second) -> {
					if (first.isInversionFor(second)) {
						return DeletePost.EMPTY;
					}
					return null;
				})
				.withTransformFunction(DeletePost.class, DeletePost.class, (left, right) -> {
					if (!left.getPostId().equals(right.getPostId())) {
						return TransformResult.of(right, left);
					}
					if (left.getTimestamp() > right.getTimestamp()) {
						return TransformResult.right(asList(right.invert(), left));
					} else if (left.getTimestamp() < right.getTimestamp()) {
						return TransformResult.left(asList(left.invert(), left));
					}
					if (left.isDelete() != right.isDelete() || !left.getDeletedBy().equals(right.getDeletedBy())) {
						return TransformResult.left(asList(left.invert(), left));
					}
					return empty();
				});
	}

	private static OTSystem<ChangeRating> ratingOTSystem() {
		OTSystem<SetValue<Boolean>> subSystem = SetValueOTSystem.create(Boolean::compareTo);
		return OTSystemImpl.<ChangeRating>create()
				.withEmptyPredicate(ChangeRating.class, op -> op.getSetRating().isEmpty())
				.withInvertFunction(ChangeRating.class, op ->
						singletonList(new ChangeRating(op.getPostId(), op.getUserId(), op.getSetRating().invert())))
				.withSquashFunction(ChangeRating.class, ChangeRating.class, (first, second) -> {
					if (!first.getPostId().equals(second.getPostId()) || first.getUserId() != second.getUserId()) {
						return null;
					}
					SetValue<Boolean> firstSetRating = first.getSetRating();
					SetValue<Boolean> secondSetRating = second.getSetRating();
					SetValue<Boolean> squashed = SetValue.set(firstSetRating.getPrev(), secondSetRating.getNext());
					return new ChangeRating(first.getPostId(), first.getUserId(), squashed);
				})
				.withTransformFunction(ChangeRating.class, ChangeRating.class, (left, right) -> {
					if (!left.getPostId().equals(right.getPostId()) || left.getUserId() != right.getUserId()) {
						return TransformResult.of(right, left);
					}
					TransformResult<SetValue<Boolean>> subResult = subSystem.transform(left.getSetRating(), right.getSetRating());
					return collect(subResult, setValue -> new ChangeRating(left.getPostId(), left.getUserId(), setValue));
				});
	}

	private static OTSystem<ChangeAttachments> attachmentsOTSystem() {
		return OTSystemImpl.<ChangeAttachments>create()
				.withEmptyPredicate(ChangeAttachments.class, op -> op.getGlobalFsId().isEmpty())
				.withInvertFunction(ChangeAttachments.class, op ->
						singletonList(op.invert()))
				.withSquashFunction(ChangeAttachments.class, ChangeAttachments.class, (first, second) -> {
					if (first.isInversionFor(second)) {
						return ChangeAttachments.EMPTY;
					}
					return null;
				})
				.withTransformFunction(ChangeAttachments.class, ChangeAttachments.class, (left, right) -> {
					if (!left.getPostId().equals(right.getPostId()) || !left.getGlobalFsId().equals(right.getGlobalFsId())) {
						return TransformResult.of(right, left);
					}
					if (!(left.isRemove() && right.isRemove()) || !left.getAttachment().equals(right.getAttachment())) {
						throw new OTTransformException("ID collision");
					}
					if (left.getTimestamp() > right.getTimestamp()) {
						return TransformResult.right(asList(right.invert(), left));
					} else if (left.getTimestamp() < right.getTimestamp()) {
						return TransformResult.left(asList(left.invert(), left));
					}
					return TransformResult.empty();
				});
	}

	private static OTSystem<ChangeContent> contentOTSystem() {
		OTSystem<ChangeName> nameOTSystem = NameOTSystem.createOTSystem();
		return OTSystemImpl.<ChangeContent>create()
				.withEmptyPredicate(ChangeContent.class, op -> nameOTSystem.isEmpty(op.getChangeContent()))
				.withInvertFunction(ChangeContent.class, op ->
						singletonList(new ChangeContent(op.getPostId(), op.getNext(), op.getPrev(), op.getTimestamp())))
				.withSquashFunction(ChangeContent.class, ChangeContent.class, (first, second) -> {
					if (!first.getPostId().equals(second.getPostId())) {
						return null;
					}
					return new ChangeContent(first.getPostId(), first.getPrev(), second.getNext(), second.getTimestamp());
				})
				.withTransformFunction(ChangeContent.class, ChangeContent.class, (left, right) -> {
					if (!left.getPostId().equals(right.getPostId())) {
						return TransformResult.of(right, left);
					}
					TransformResult<ChangeName> subResult = nameOTSystem.transform(
							left.getChangeContent(), right.getChangeContent());
					return collect(subResult, changeName -> new ChangeContent(left.getPostId(), changeName));
				});
	}

	private static <O, S> TransformResult<O> collect(TransformResult<S> subResult, Function<S, O> constructor) {
		return TransformResult.of(doCollect(subResult.left, constructor), doCollect(subResult.right, constructor));
	}

	private static <O, S> List<O> doCollect(List<S> ops, Function<S, O> constructor) {
		return ops.stream()
				.map(constructor)
				.collect(toList());
	}

	private static <O> List<O> doFilter(List<O> ops, Predicate<O> predicate) {
		return ops.stream()
				.filter(predicate)
				.collect(toList());
	}

	private static PostChangesOperation extractOpsById(String id, PostChangesOperation changes) {
		List<ChangeContent> changeContents = doFilter(changes.getChangeContentOps(), op -> op.getPostId().equals(id));
		List<ChangeAttachments> changeAttachments = doFilter(changes.getChangeAttachmentsOps(), op -> op.getPostId().equals(id));
		List<ChangeRating> changeRating = doFilter(changes.getChangeRatingOps(), op -> op.getPostId().equals(id));
		List<ChangeLastEditTimestamp> changeTimestamp = doFilter(changes.getChangeLastEditTimestamps(), op -> op.getPostId().equals(id));
		List<DeletePost> deletePost = doFilter(changes.getDeletePostOps(), op -> op.getPostId().equals(id));
		return new PostChangesOperation(changeContents, changeAttachments, changeRating, deletePost, changeTimestamp);
	}
}
