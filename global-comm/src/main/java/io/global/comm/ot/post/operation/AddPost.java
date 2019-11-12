package io.global.comm.ot.post.operation;

import io.global.comm.pojo.Post;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.global.ot.session.AuthService.DK_APP_STORE;

public final class AddPost implements ThreadOperation {
	public static final AddPost EMPTY = new AddPost("", null, new UserId(DK_APP_STORE, ""), -1, false);

	private final String postId;
	@Nullable
	private final String parentId;
	private final UserId author;
	private final long initialTimestamp;
	private final boolean remove;

	public AddPost(String postId, @Nullable String parentId, UserId author, long initialTimestamp, boolean remove) {
		this.postId = postId;
		this.parentId = parentId;
		this.author = author;
		this.initialTimestamp = initialTimestamp;
		this.remove = remove;
	}

	public static AddPost addPost(String postId, @Nullable String parentId, UserId author, long initialTimestamp) {
		checkArgument(initialTimestamp != -1, "Trying to create empty operation");
		return new AddPost(postId, parentId, author, initialTimestamp, false);
	}

	@Override
	public void apply(Map<String, Post> posts) {
		if (remove) {
			Post post = posts.remove(postId);
			if (parentId != null) {
				posts.get(parentId).removeChild(post);
			}
		} else {
			Post post = Post.create(postId, author, initialTimestamp);
			posts.put(postId, post);
			if (parentId != null) {
				posts.get(parentId).addChild(post);
			}
		}
	}

	public AddPost invert() {
		return new AddPost(postId, parentId, author, initialTimestamp, !remove);
	}

	public boolean isInversion(AddPost other) {
		return Objects.equals(parentId, other.parentId) &&
				postId.equals(other.postId) &&
				author.equals(other.author) &&
				initialTimestamp == other.initialTimestamp &&
				remove != other.remove;
	}

	public String getPostId() {
		return postId;
	}

	@Nullable
	public String getParentId() {
		return parentId;
	}

	public UserId getAuthor() {
		return author;
	}

	public long getInitialTimestamp() {
		return initialTimestamp;
	}

	public boolean isRemove() {
		return remove;
	}

	@Override
	public String toString() {
		return "AddPost{" +
				"postId=" + postId +
				", parentId=" + parentId +
				", author=" + author +
				", initialTimestamp=" + initialTimestamp +
				", remove=" + remove +
				'}';
	}
}
