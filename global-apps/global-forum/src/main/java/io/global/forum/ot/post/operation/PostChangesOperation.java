package io.global.forum.ot.post.operation;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.global.ot.map.SetValue.set;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class PostChangesOperation implements PostOperation {
	public static final PostChangesOperation EMPTY = new PostChangesOperation(emptyList(), emptyList(), emptyList(), emptyList(), emptyList());
	public static final StructuredCodec<PostChangesOperation> CODEC = StructuredCodecs.tuple(PostChangesOperation::new,
			PostChangesOperation::getChangeContentOps, ofList(ChangeContent.CODEC),
			PostChangesOperation::getChangeAttachmentsOps, ofList(ChangeAttachments.CODEC),
			PostChangesOperation::getChangeRatingOps, ofList(ChangeRating.CODEC),
			PostChangesOperation::getDeletePostOps, ofList(DeletePost.CODEC),
			PostChangesOperation::getChangeLastEditTimestamps, ofList(ChangeLastEditTimestamp.CODEC));

	private final List<ChangeContent> changeContentOps;
	private final List<ChangeAttachments> changeAttachmentsOps;
	private final List<ChangeRating> changeRatingOps;
	private final List<DeletePost> deletePostOps;
	private final List<ChangeLastEditTimestamp> changeLastEditTimestamps;

	public PostChangesOperation(List<ChangeContent> changeContentOps, List<ChangeAttachments> changeAttachmentsOps, List<ChangeRating> changeRatingOps, List<DeletePost> deletePostOps, List<ChangeLastEditTimestamp> changeLastEditTimestamps) {
		this.changeContentOps = changeContentOps;
		this.changeAttachmentsOps = changeAttachmentsOps;
		this.changeRatingOps = changeRatingOps;
		this.deletePostOps = deletePostOps;
		this.changeLastEditTimestamps = changeLastEditTimestamps;
	}

	public static PostChangesOperation forNewPost(long postId, String content, Map<String, Attachment> attachments, long initialTimestamp) {
		List<ChangeAttachments> attachmentsOps = attachmentsToOps(postId, attachments, initialTimestamp, false);
		ChangeContent changeContent = new ChangeContent(postId, "", content, initialTimestamp);
		return new PostChangesOperation(singletonList(changeContent), attachmentsOps, emptyList(), emptyList(), emptyList());
	}

	public static PostChangesOperation content(long postId, String prev, String next, long prevTimestamp, long nextTimestamp) {
		ChangeContent changeContent = new ChangeContent(postId, prev, next, nextTimestamp);
		ChangeLastEditTimestamp changeLastEditTimestamp = new ChangeLastEditTimestamp(postId, prevTimestamp, nextTimestamp);
		return new PostChangesOperation(singletonList(changeContent), emptyList(), emptyList(), emptyList(), singletonList(changeLastEditTimestamp));
	}

	public static PostChangesOperation changeAttachments(long postId, Map<String, Attachment> attachments, long prevTimestamp, long nextTimestamp, boolean remove) {
		List<ChangeAttachments> attachmentsOps = attachmentsToOps(postId, attachments, nextTimestamp, remove);
		ChangeLastEditTimestamp changeLastEditTimestamp = new ChangeLastEditTimestamp(postId, prevTimestamp, nextTimestamp);
		return new PostChangesOperation(emptyList(), attachmentsOps, emptyList(), emptyList(), singletonList(changeLastEditTimestamp));
	}

	public static PostChangesOperation rating(long postId, UserId userId, @Nullable Boolean prev, @Nullable Boolean next) {
		ChangeRating changeRating = new ChangeRating(postId, userId, set(prev, next));
		return new PostChangesOperation(emptyList(), emptyList(), singletonList(changeRating), emptyList(), emptyList());
	}

	public static PostChangesOperation delete(DeletePost deleteOp, ChangeLastEditTimestamp timestampOp) {
		return new PostChangesOperation(emptyList(), emptyList(), emptyList(), singletonList(deleteOp), singletonList(timestampOp));
	}

	public static PostChangesOperation delete(long postId, UserId deletedBy, long prevTimestamp, long nextTimestamp) {
		DeletePost deletePost = DeletePost.delete(postId, deletedBy, nextTimestamp);
		ChangeLastEditTimestamp changeLastEditTimestamp = new ChangeLastEditTimestamp(postId, prevTimestamp, nextTimestamp);
		return new PostChangesOperation(emptyList(), emptyList(), emptyList(), singletonList(deletePost), singletonList(changeLastEditTimestamp));
	}

	public static PostChangesOperation lastEdit(ChangeLastEditTimestamp op) {
		return new PostChangesOperation(emptyList(), emptyList(), emptyList(), emptyList(), singletonList(op));
	}

	@Override
	public void apply(Map<Long, Post> posts) {
		changeContentOps.forEach(op -> op.apply(posts));
		changeAttachmentsOps.forEach(op -> op.apply(posts));
		changeRatingOps.forEach(op -> op.apply(posts));
		deletePostOps.forEach(op -> op.apply(posts));
		changeLastEditTimestamps.forEach(op -> op.apply(posts));
	}

	public List<ChangeContent> getChangeContentOps() {
		return changeContentOps;
	}

	public List<ChangeAttachments> getChangeAttachmentsOps() {
		return changeAttachmentsOps;
	}

	public List<ChangeRating> getChangeRatingOps() {
		return changeRatingOps;
	}

	public List<DeletePost> getDeletePostOps() {
		return deletePostOps;
	}

	public List<ChangeLastEditTimestamp> getChangeLastEditTimestamps() {
		return changeLastEditTimestamps;
	}

	public boolean isEmpty() {
		return changeContentOps.isEmpty() && changeAttachmentsOps.isEmpty() &&
				changeRatingOps.isEmpty() && deletePostOps.isEmpty() &&
				changeLastEditTimestamps.isEmpty();
	}

	private static List<ChangeAttachments> attachmentsToOps(long postId, Map<String, Attachment> attachments, long timestamp, boolean remove) {
		return attachments.entrySet().stream()
				.map(entry -> new ChangeAttachments(postId, entry.getKey(), entry.getValue(), timestamp, remove))
				.collect(Collectors.toList());
	}

	@Override
	public String toString() {
		return "PostChangesOperation{" +
				"contentOps=" + changeContentOps +
				", attachmentsOps=" + changeAttachmentsOps +
				", ratingOps=" + changeRatingOps +
				", deleteOps=" + deletePostOps +
				", lastEditTimestamps=" + changeLastEditTimestamps +
				'}';
	}
}
