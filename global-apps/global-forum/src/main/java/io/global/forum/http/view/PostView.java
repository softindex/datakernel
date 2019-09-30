package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.Rating;
import io.global.comm.pojo.UserId;
import io.global.comm.pojo.UserRole;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;

import static io.datakernel.util.CollectorsEx.toMultimap;
import static io.global.forum.util.Utils.formatInstant;
import static java.util.Map.Entry.comparingByKey;

public final class PostView {
	private static final Comparator<Post> POST_COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);

	private final String postId;
	private final String content;
	private final String initialTimestamp;
	private final String lastEditTimestamp;
	private final Instant timestamp;

	private final UserView author;
	private final Map<String, Set<String>> attachments;

	@Nullable
	private final UserView deletedBy;

	private final boolean own;
	private final boolean editable;
	private final boolean deletedVisible;

	private final int likes;
	private final boolean liked;
	private final boolean disliked;

	private final List<PostView> children;
	private final boolean hasChildren;

	public PostView(
			String postId, String content, String initialTimestamp, String lastEditTimestamp, Instant timestamp,
			UserView author,
			Map<String, Set<String>> attachments,
			@Nullable UserView deletedBy,
			boolean own, boolean editable, boolean deletedVisible,
			int likes, boolean liked, boolean disliked,
			List<PostView> children,
			boolean hasChildren) {
		this.postId = postId;
		this.content = content;
		this.initialTimestamp = initialTimestamp;
		this.lastEditTimestamp = lastEditTimestamp;
		this.timestamp = timestamp;
		this.author = author;
		this.attachments = attachments;
		this.deletedBy = deletedBy;
		this.own = own;
		this.editable = editable;
		this.deletedVisible = deletedVisible;
		this.likes = likes;
		this.liked = liked;
		this.disliked = disliked;
		this.children = children;
		this.hasChildren = hasChildren;
	}

	public String getPostId() {
		return postId;
	}

	public String getContent() {
		return content;
	}

	public String getInitialTimestamp() {
		return initialTimestamp;
	}

	public String getLastEditTimestamp() {
		return lastEditTimestamp;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public UserView getAuthor() {
		return author;
	}

	public Map<String, Set<String>> getAttachments() {
		return attachments;
	}

	@Nullable
	public UserView getDeletedBy() {
		return deletedBy;
	}

	public boolean isOwn() {
		return own;
	}

	public boolean isEditable() {
		return editable;
	}

	public boolean isDeletedVisible() {
		return deletedVisible;
	}

	public int getLikes() {
		return likes;
	}

	public boolean isLiked() {
		return liked;
	}

	public boolean isDisliked() {
		return disliked;
	}

	public List<PostView> getChildren() {
		return children;
	}

	public boolean hasChildren() {
		return hasChildren;
	}

	public static Promise<PostView> from(CommDao commDao, Post post, @Nullable UserId currentUser, UserRole currentRole, int depth) {
		assert depth >= 0;

		Promise<List<PostView>> childrenPromise = depth != 0 ?
				Promises.toList(post.getChildren().stream().sorted(POST_COMPARATOR).map(p -> from(commDao, p, currentUser, currentRole, depth - 1))) :
				Promise.of(Collections.<PostView>emptyList());

		return childrenPromise
				.then(children -> {
					UserId deleterId = post.getDeletedBy();
					Promise<UserView> authorPromise = UserView.from(commDao, post.getAuthor());
					Promise<@Nullable UserView> deleterPromise = UserView.from(commDao, deleterId);
					return Promises.toTuple(authorPromise, deleterPromise)
							.map(users -> {
								UserView author = users.getValue1();
								UserView deleter = users.getValue2();

								boolean own = post.getAuthor().equals(currentUser);
								Map<Rating, Set<UserId>> ratings = post.getRatings();
								Set<UserId> likes = ratings.get(Rating.LIKE);
								Set<UserId> dislikes = ratings.get(Rating.DISLIKE);

								long initialTimestamp = post.getInitialTimestamp();
								long lastEditTimestamp = post.getLastEditTimestamp();
								Instant timestamp = Instant.ofEpochMilli(lastEditTimestamp == -1 ? initialTimestamp : lastEditTimestamp);

								return new PostView(
										post.getId(),
										post.getContent(),
										formatInstant(initialTimestamp),
										formatInstant(lastEditTimestamp),
										timestamp,
										author,
										post.getAttachments().entrySet().stream()
												.sorted(comparingByKey())
												.collect(toMultimap(entry -> entry.getValue().toString().toLowerCase(), Entry::getKey)),
										deleter,
										post.getAuthor().equals(currentUser),
										currentRole.isPrivileged() || own && currentRole.isKnown() && (deleterId == null || post.getAuthor().equals(deleterId)),
										currentRole.isPrivileged() || own && currentRole.isKnown(),
										likes.size() - dislikes.size(),
										currentUser != null && likes.contains(currentUser),
										currentUser != null && dislikes.contains(currentUser),
										children,
										!post.getChildren().isEmpty());
							});
				});
	}
}
