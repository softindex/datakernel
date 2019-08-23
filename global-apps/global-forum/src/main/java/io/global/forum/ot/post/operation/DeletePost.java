package io.global.forum.ot.post.operation;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.BOOLEAN_CODEC;
import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.global.forum.pojo.AuthService.DK_APP_STORE;

public final class DeletePost implements PostOperation {
	public static final DeletePost EMPTY = new DeletePost(0, new UserId(DK_APP_STORE, ""), -1, true);
	public static final StructuredCodec<DeletePost> CODEC = StructuredCodecs.tuple(DeletePost::new,
			DeletePost::getPostId, LONG_CODEC,
			DeletePost::getDeletedBy, UserId.CODEC,
			DeletePost::getTimestamp, LONG_CODEC,
			DeletePost::isDelete, BOOLEAN_CODEC);

	private final long postId;
	private final UserId deletedBy;
	private final long timestamp;
	private final boolean delete;

	private DeletePost(long postId, UserId deletedBy, long timestamp, boolean delete) {
		this.postId = postId;
		this.deletedBy = deletedBy;
		this.timestamp = timestamp;
		this.delete = delete;
	}

	public static DeletePost delete(long postId, UserId deletedBy, long timestamp) {
		return new DeletePost(postId, deletedBy, timestamp, true);
	}

	@Override
	public void apply(Map<Long, Post> posts) {
		Post post = posts.get(postId);
		if (delete) {
			post.delete(deletedBy);
		} else {
			post.unDelete();
		}
	}

	public DeletePost invert() {
		return new DeletePost(postId, deletedBy, timestamp, !delete);
	}

	public boolean isInversionFor(DeletePost other) {
		return postId == other.postId &&
				deletedBy.equals(other.deletedBy) &&
				timestamp == other.timestamp &&
				delete != other.delete;
	}

	public long getPostId() {
		return postId;
	}

	public UserId getDeletedBy() {
		return deletedBy;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isDelete() {
		return delete;
	}

	@Override
	public String toString() {
		return (delete ? "Delete" : "UnDelete") +
				"Post{" +
				"postId=" + postId +
				", by=" + deletedBy +
				", timestamp=" + timestamp +
				'}';
	}
}
