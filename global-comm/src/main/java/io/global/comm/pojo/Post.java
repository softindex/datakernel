package io.global.comm.pojo;

import io.global.ot.session.UserId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.global.util.Utils.limit;

public final class Post {
	private final String id;
	@NotNull
	private final UserId author;
	private String content = "";
	private final long initialTimestamp;
	private long lastEditTimestamp = -1;

	private final Map<Rating, Set<UserId>> ratings = new EnumMap<>(Rating.class);

	private final Map<String, AttachmentType> attachments = new HashMap<>();

	@Nullable
	private Post parent;
	private final List<Post> children = new ArrayList<>();

	@Nullable
	private UserId deletedBy;

	private Post(String id, @NotNull UserId author, long initialTimestamp) {
		this.id = id;
		this.author = author;
		this.initialTimestamp = initialTimestamp;

		for (Rating value : Rating.values()) {
			ratings.put(value, new HashSet<>());
		}
	}

	public static Post create(String postId, @NotNull UserId author, long initialTimestamp) {
		return new Post(postId, author, initialTimestamp);
	}

	public String getId() {
		return id;
	}

	// Mutates child
	public void addChild(@NotNull Post child) {
		checkArgument(child.parent == null, "Child has other parent");
		this.children.add(child);
		child.parent = this;
	}

	public void removeChild(@NotNull Post child) {
		this.children.remove(child);
	}

	public void setContent(@NotNull String content) {
		this.content = content;
	}

	public void addAttachment(String filename, AttachmentType attachmentType) {
		this.attachments.put(filename, attachmentType);
	}

	public void removeAttachment(String filename) {
		this.attachments.remove(filename);
	}

	public void setLastEditTimestamp(long lastEditTimestamp) {
		this.lastEditTimestamp = lastEditTimestamp;
	}

	public void updateRating(UserId userId, @Nullable Rating rating) {
		if (rating == null) {
			ratings.values().forEach(set -> set.remove(userId));
			return;
		}
		if (ratings.get(rating).add(userId)) {
			ratings.forEach((key, value) -> {
				if (key != rating) {
					value.remove(userId);
				}
			});
		}
	}

	public void delete(@NotNull UserId deletedBy) {
		this.deletedBy = deletedBy;
	}

	public void unDelete() {
		this.deletedBy = null;
	}

	// region getters
	@NotNull
	public UserId getAuthor() {
		return author;
	}

	public long getInitialTimestamp() {
		return initialTimestamp;
	}

	@NotNull
	public Map<String, AttachmentType> getAttachments() {
		return attachments;
	}

	@NotNull
	public String getContent() {
		return content;
	}

	public long getLastEditTimestamp() {
		return lastEditTimestamp;
	}

	@Nullable
	public UserId getDeletedBy() {
		return deletedBy;
	}

	@Nullable
	public Post getParent() {
		return parent;
	}

	public int computeDepth() {
		int depth = 0;
		Post post = this;
		while ((post = post.parent) != null) {
			depth++;
		}
		return depth;
	}

	public int countAllPosts() {
//		return 1 + children.stream().mapToInt(Post::countLeafChildren).sum();

		// this was just a bit fun to implement without recursion

		int number = 1;
		Stack<List<Post>> stack = new Stack<>();
		if (!children.isEmpty()) {
			stack.add(children);
		}
		while (!stack.isEmpty()) {
			List<Post> list = stack.pop();
			number += list.size();
			for (Post child : list) {
				List<Post> subchildren = child.getChildren();
				if (!subchildren.isEmpty()) {
					stack.push(subchildren);
				}
			}
		}
		return number;
	}

	public List<Post> getChildren() {
		return children;
	}

	@Nullable
	public Rating getRating(UserId userId) {
		for (Map.Entry<Rating, Set<UserId>> entry : ratings.entrySet()) {
			if (entry.getValue().contains(userId)) {
				return entry.getKey();
			}
		}
		return null;
	}

	public Map<Rating, Set<UserId>> getRatings() {
		return ratings;
	}
	// endregion

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Post post = (Post) o;

		if (initialTimestamp != post.initialTimestamp) return false;
		if (lastEditTimestamp != post.lastEditTimestamp) return false;
		if (!children.equals(post.children)) return false;
		if (!author.equals(post.author)) return false;
		if (!ratings.equals(post.ratings)) return false;
		if (!Objects.equals(parent, post.parent)) return false;
		if (!content.equals(post.content)) return false;
		if (!attachments.equals(post.attachments)) return false;
		return Objects.equals(deletedBy, post.deletedBy);
	}

	@Override
	public int hashCode() {
		int result = children.hashCode();
		result = 31 * result + author.hashCode();
		result = 31 * result + (int) (initialTimestamp ^ (initialTimestamp >>> 32));
		result = 31 * result + ratings.hashCode();
		result = 31 * result + (parent != null ? parent.hashCode() : 0);
		result = 31 * result + content.hashCode();
		result = 31 * result + attachments.hashCode();
		result = 31 * result + (deletedBy != null ? deletedBy.hashCode() : 0);
		result = 31 * result + (int) (lastEditTimestamp ^ (lastEditTimestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "Post{" +
				"children=" + children +
				", author=" + author +
				", initialTimestamp=" + initialTimestamp +
				", ratings=" + ratings +
				", parent=" + parent +
				", content='" + limit(content, 20) + '\'' +
				", attachments=" + attachments +
				", deletedBy=" + deletedBy +
				", lastEditTimestamp=" + lastEditTimestamp +
				'}';
	}
}
