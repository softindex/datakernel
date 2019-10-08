package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.Rating;
import io.global.comm.pojo.UserId;
import io.global.comm.pojo.UserRole;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.util.CollectorsEx.toMultimap;
import static io.global.forum.util.Utils.formatInstant;
import static java.util.Collections.emptyList;
import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.toList;

public final class PostView implements Comparable<PostView> {
	private static final Comparator<Post> POST_COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);

	private final String id;
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

	@Nullable
	private PostView inlineParent;

	public PostView(
			String id, String content, String initialTimestamp, String lastEditTimestamp, Instant timestamp,
			UserView author,
			Map<String, Set<String>> attachments,
			@Nullable UserView deletedBy,
			boolean own, boolean editable, boolean deletedVisible,
			int likes, boolean liked, boolean disliked,
			List<PostView> children) {
		this.id = id;
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
	}

	public String getPostId() {
		return id;
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

	@Nullable
	public PostView getInlineParent() {
		return inlineParent;
	}

	public void setInlineParent(@Nullable PostView inlineParent) {
		this.inlineParent = inlineParent;
	}

	@Override
	public int compareTo(@NotNull PostView o) {
		return timestamp.compareTo(o.timestamp);
	}

	private static Promise<Stream<PostView>> inlineChildren(CommDao commDao, Post post, @Nullable UserId currentUser, UserRole currentRole, @Nullable PostView parent) {
		return Promises.toList(post.getChildren().stream()
				.map(p ->
						from(commDao, p, currentUser, currentRole, -1)
								.then(pv -> {
									pv.setInlineParent(parent);
									return inlineChildren(commDao, p, currentUser, currentRole, pv).map(s -> Stream.concat(Stream.of(pv), s));
								})))
				.map(ll -> ll.stream().flatMap(Function.identity()).sorted());
	}

	public static Promise<PostView> single(CommDao commDao, Post post, @Nullable UserId currentUser, UserRole currentRole, int maxDepth) {
		Promise<PostView> promise = from(commDao, post, currentUser, currentRole, -1);
		if (maxDepth != -1 && post.computeDepth() > maxDepth + 1) { // implies post.getParent() != null
			return Promises.toTuple(promise, single(commDao, post.getParent(), currentUser, currentRole, -1))
					.map(t -> {
						t.getValue1().setInlineParent(t.getValue2());
						return t.getValue1();
					});
		}
		return promise;
	}

	public static Promise<PostView> from(CommDao commDao, Post post, @Nullable UserId currentUser, UserRole currentRole, int depth) {
		assert depth >= -1;

		if (post == null) {
			return Promise.of(null);
		}

		boolean deepest = depth == 0;

		Promise<List<PostView>> childrenPromise =
				depth == -1 ?
						Promise.of(emptyList()) :
						deepest ?
								inlineChildren(commDao, post, currentUser, currentRole, null).map(s -> s.collect(toList())) :
								Promises.toList(post.getChildren().stream().sorted(POST_COMPARATOR).map(p -> from(commDao, p, currentUser, currentRole, depth - 1)));

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

								return new PostView(
										post.getId(),
										post.getContent(),
										formatInstant(initialTimestamp),
										formatInstant(lastEditTimestamp),
										Instant.ofEpochMilli(initialTimestamp),
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
										children
								);
							});
				});
	}
}
