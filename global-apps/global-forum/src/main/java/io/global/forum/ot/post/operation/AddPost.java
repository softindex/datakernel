package io.global.forum.ot.post.operation;

import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.global.forum.pojo.AuthService.DK_APP_STORE;

public final class AddPost implements ThreadOperation {
	public static final AddPost EMPTY = new AddPost(0L, null, new UserId(DK_APP_STORE, ""), -1, false);

	private final Long postId;
	@Nullable
	private final Long parentId;
	private final UserId author;
	private final long initialTimestamp;
	private final boolean remove;

	public AddPost(Long postId, @Nullable Long parentId, UserId author, long initialTimestamp, boolean remove) {
		this.postId = postId;
		this.parentId = parentId;
		this.author = author;
		this.initialTimestamp = initialTimestamp;
		this.remove = remove;
	}

	public static AddPost addPost(Long postId, @Nullable Long parentId, UserId author, long initialTimestamp) {
		checkArgument(initialTimestamp != -1, "Trying to create empty operation");
		return new AddPost(postId, parentId, author, initialTimestamp, false);
	}

	@Override
	public void apply(Map<Long, Post> posts) {
		if (remove) {
			Post post = posts.remove(postId);
			if (parentId != null) {
				posts.get(parentId).removeChild(post);
			}
		} else {
			Post post = Post.create(author, initialTimestamp);
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

	public Long getPostId() {
		return postId;
	}

	@Nullable
	public Long getParentId() {
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
