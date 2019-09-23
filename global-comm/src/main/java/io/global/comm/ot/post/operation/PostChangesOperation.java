package io.global.comm.ot.post.operation;

import io.global.comm.pojo.AttachmentType;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.Rating;
import io.global.comm.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.global.ot.map.SetValue.set;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class PostChangesOperation implements ThreadOperation {
	public static final PostChangesOperation EMPTY = new PostChangesOperation(emptyList(), emptyList(), emptyList(), emptyList(), emptyList());

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

	public static PostChangesOperation forNewPost(String postId, String content, Map<String, AttachmentType> attachments, long initialTimestamp) {
		List<ChangeAttachments> attachmentsOps = attachmentsToOps(postId, attachments, initialTimestamp, false);
		ChangeContent changeContent = new ChangeContent(postId, "", content, initialTimestamp);
		return new PostChangesOperation(singletonList(changeContent), attachmentsOps, emptyList(), emptyList(), emptyList());
	}

	public static PostChangesOperation rating(String postId, UserId userId, @Nullable Rating prev, @Nullable Rating next) {
		ChangeRating changeRating = new ChangeRating(postId, userId, set(prev, next));
		return new PostChangesOperation(emptyList(), emptyList(), singletonList(changeRating), emptyList(), emptyList());
	}

	public static PostChangesOperation delete(String postId, UserId deletedBy, long prevTimestamp, long nextTimestamp) {
		DeletePost deletePost = DeletePost.delete(postId, deletedBy, nextTimestamp);
		ChangeLastEditTimestamp changeLastEditTimestamp = new ChangeLastEditTimestamp(postId, prevTimestamp, nextTimestamp);
		return new PostChangesOperation(emptyList(), emptyList(), emptyList(), singletonList(deletePost), singletonList(changeLastEditTimestamp));
	}

	public static PostChangesOperation restore(String postId, UserId deletedBy, long prevTimestamp, long nextTimestamp) {
		DeletePost restorePost = DeletePost.restore(postId, deletedBy, nextTimestamp);
		ChangeLastEditTimestamp changeLastEditTimestamp = new ChangeLastEditTimestamp(postId, prevTimestamp, nextTimestamp);
		return new PostChangesOperation(emptyList(), emptyList(), emptyList(), singletonList(restorePost), singletonList(changeLastEditTimestamp));
	}

	@Override
	public void apply(Map<String, Post> posts) {
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

	public static List<ChangeAttachments> attachmentsToOps(String postId, Map<String, AttachmentType> attachments, long timestamp, boolean remove) {
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
