package io.global.blog.http.view;

import io.datakernel.common.tuple.Tuple3;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.*;
import io.global.common.CryptoUtils;
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static io.datakernel.common.CollectorsEx.toMultimap;

public final class PostView {
	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final Comparator<Post> POST_COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);
	private static final MessageDigest MD5;

	static {
		try {
			MD5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ignored) {
			throw new AssertionError("Apparently, MD5 algorithm does not exist");
		}
	}

	private final String postId;
	private final String initialTimestamp;
	private final String lastEditTimestamp;
	private final String author;
	private final String authorId;
	private final String avatarUrl;
	@Nullable
	private final String deletedBy;
	private final boolean deletedVisible;
	private final boolean editable;
	private final boolean deepest;
	private final Map<String, Set<String>> attachments;
	@Nullable
	private final PostView parent;

	private List<PostView> children;
	@Nullable
	private String renderedContent;
	private String content;


	public PostView(
			String postId, String content, boolean deepest, String initialTimestamp, String lastEditTimestamp,
			String author, String authorId, String avatarUrl,
			Map<String, Set<String>> attachments,
			@Nullable String deletedBy, @Nullable PostView parent,
			boolean editable, boolean deletedVisible) {
		this.postId = postId;
		this.content = content;
		this.initialTimestamp = initialTimestamp;
		this.lastEditTimestamp = lastEditTimestamp;
		this.author = author;
		this.authorId = authorId;
		this.avatarUrl = avatarUrl;
		this.attachments = attachments;
		this.deletedBy = deletedBy;
		this.editable = editable;
		this.parent = parent;
		this.deletedVisible = deletedVisible;
		this.deepest = deepest;
	}

	private static String format(long timestamp) {
		return timestamp == -1 ? "" : Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	private static String md5(@Nullable String str) {
		if (str == null) {
			return null;
		}
		// we are single-threaded so far
//		synchronized (MD5) {
		MD5.update(str.getBytes());
		return CryptoUtils.toHexString(MD5.digest());
//		}
	}

	private static String avatarUrl(@Nullable UserData data) {
		if (data == null) {
			return "https://gravatar.com/avatar?d=mp";
		}
		String email = data.getEmail();
		return email == null ?
				"https://gravatar.com/avatar?d=mp" :
				"https://gravatar.com/avatar/" + md5(email) + "?d=identicon";
	}

	public static Promise<PostView> from(CommDao commDao, Post post, @Nullable UserId currentUser, int childrenDepth, @Nullable PostView parent) {
		boolean deepest = childrenDepth != 0;
		UserId deleterId = post.getDeletedBy();
		Promise<@Nullable UserData> authorPromise = commDao.getUser(post.getAuthor());
		Promise<@Nullable UserData> deleterPromise = deleterId != null ? commDao.getUser(deleterId) : Promise.of(null);
		Promise<@Nullable UserData> currentPromise = currentUser != null ? commDao.getUser(currentUser) : Promise.of(null);
		return Promises.toTuple(authorPromise, deleterPromise, currentPromise)
				.map(users -> ofTuple(users, post, currentUser, deleterId, deepest, parent))
				.then(root -> {
					Promise<List<PostView>> childrenPromise = deepest ?
							Promises.toList(post.getChildren().stream().sorted(POST_COMPARATOR).map(p -> from(commDao, p, currentUser, childrenDepth - 1, root))) :
							Promise.of(Collections.<PostView>emptyList());
					return childrenPromise.map(root::withChildren);
				});
	}

	private static PostView ofTuple(Tuple3<UserData, UserData, UserData> users, Post post,
									@Nullable UserId currentUser, @Nullable UserId deleterUser,
									boolean deepest, @Nullable PostView parent) {
		UserData author = users.getValue1();
		UserData deleter = users.getValue2();
		UserData current = users.getValue3();
		UserRole role = current != null ? current.getRole() : UserRole.GUEST;
		boolean own = post.getAuthor().equals(currentUser);
		boolean editable = role.isPrivileged() || own && role.isKnown() && (deleterUser == null || post.getAuthor().equals(deleterUser));
		boolean deletedVisible = role.isPrivileged() || own && role.isKnown();
		return new PostView(post.getId(), post.getContent(), deepest,
				format(post.getInitialTimestamp()),
				format(post.getLastEditTimestamp()),
				author != null ? author.getUsername() : null,
				post.getAuthor().getAuthId(),
				avatarUrl(author),
				convert(post.getAttachments()),
				deleter != null ? deleter.getUsername() : null,
				parent,
				editable,
				deletedVisible
		);
	}

	public String getPostId() {
		return postId;
	}

	public String getAuthor() {
		return author;
	}

	public String getAuthorId() {
		return authorId;
	}

	public String getInitialTimestamp() {
		return initialTimestamp;
	}

	public String getLastEditTimestamp() {
		return lastEditTimestamp;
	}

	public List<PostView> getChildren() {
		return children;
	}

	public Map<String, Set<String>> getAttachments() {
		return attachments;
	}

	@Nullable
	public String getDeletedBy() {
		return deletedBy;
	}

	public String getAvatarUrl() {
		return avatarUrl;
	}

	public boolean isEditable() {
		return editable;
	}

	public boolean isDeletedVisible() {
		return deletedVisible;
	}

	public boolean isDeepest() {
		return deepest;
	}

	public PostView withRenderedContent(String renderedContent) {
		if (this.renderedContent == null) {
			this.renderedContent = renderedContent;
		}
		return this;
	}

	public PostView withContent(String content) {
		this.content = content;
		return this;
	}

	public String getContent() {
		return content;
	}

	@Nullable
	public String getRenderedContent() {
		return this.renderedContent;
	}

	@Nullable
	public PostView getParent() {
		return parent;
	}

	public PostView withChildren(List<PostView> children) {
		this.children = children;
		return this;
	}


	private static Map<String, Set<String>> convert(Map<String, AttachmentType> attachments) {
		return attachments.entrySet().stream()
				.sorted(Comparator.comparing(Map.Entry::getKey))
				.collect(toMultimap(entry -> entry.getValue().toString().toLowerCase(), Map.Entry::getKey));
	}
}
