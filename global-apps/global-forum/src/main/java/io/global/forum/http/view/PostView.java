package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
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
import java.util.Map.Entry;

import static io.datakernel.util.CollectorsEx.toMultimap;

public final class PostView {
	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final Comparator<Entry<String, Attachment>> ATTACHMENT_COMPARATOR = Comparator.comparing(a -> a.getValue().getFilename());
	private static final Comparator<Post> POST_COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);

	private final String postId;
	private final String author;
	private final String authorId;
	private final String content;
	private final String initialTimestamp;
	private final String lastEditTimestamp;

	private final List<PostView> children;
	private final Map<String, Set<AttachmentView>> attachments;

	private final String avatarUrl;

	@Nullable
	private final String deletedBy;

	private final boolean editable;
	private final boolean deletedVisible;

	private final boolean editedNow;

	public PostView(String postId, String author, String authorId, String content, String initialTimestamp, String lastEditTimestamp,
			List<PostView> children, Map<String, Set<AttachmentView>> attachments, String avatarUrl, @Nullable String deletedBy,
			boolean editable, boolean deletedVisible, boolean editedNow) {
		this.postId = postId;
		this.author = author;
		this.authorId = authorId;
		this.content = content;
		this.initialTimestamp = initialTimestamp;
		this.lastEditTimestamp = lastEditTimestamp;
		this.children = children;
		this.attachments = attachments;
		this.avatarUrl = avatarUrl;
		this.deletedBy = deletedBy;
		this.editable = editable;
		this.deletedVisible = deletedVisible;
		this.editedNow = editedNow;
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

	public String getContent() {
		return content;
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

	public Map<String, Set<AttachmentView>> getAttachments() {
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

	public boolean isEditedNow() {
		return editedNow;
	}

	private static String format(long timestamp) {
		return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	// TODO anton: add recursion hard stop condition (like >100 child depth) and proper view/pagination
	public static Promise<PostView> from(CommDao commDao, Post post, @Nullable UserId currentUser, @Nullable String editedPostId, boolean includeChildren) {
		Promise<List<PostView>> childrenPromise = includeChildren ?
				Promises.toList(post.getChildren().stream().sorted(POST_COMPARATOR).map(p -> from(commDao, p, currentUser, editedPostId, true))) :
				Promise.of(Collections.<PostView>emptyList());
		return childrenPromise
				.then(children -> {
					UserId deleterId = post.getDeletedBy();
					Promise<@Nullable UserData> authorPromise = commDao.getUser(post.getAuthor());
					Promise<@Nullable UserData> deleterPromise = deleterId != null ? commDao.getUser(deleterId) : Promise.of(null);
					Promise<@Nullable UserData> currentPromise = currentUser != null ? commDao.getUser(currentUser) : Promise.of(null);
					return Promises.toTuple(authorPromise, deleterPromise, currentPromise)
							.map(users -> {
								UserData author = users.getValue1();
								UserData deleter = users.getValue2();
								UserData current = users.getValue3();
								UserRole role = current != null ? current.getRole() : UserRole.GUEST;

								boolean own = post.getAuthor().equals(currentUser);
								return new PostView(
										post.getId(),
										author != null ? author.getUsername() : null,
										post.getAuthor().getAuthId(),
										post.getContent(),
										format(post.getInitialTimestamp()),
										format(post.getLastEditTimestamp()),
										children,
										convert(post.getAttachments()),
										avatarUrl(author),
										deleter != null ? deleter.getUsername() : null,
										role.isPrivileged() || own && role.isKnown() && (deleterId == null || post.getAuthor().equals(deleterId)),
										role.isPrivileged() || own && role.isKnown(),
										post.getId().equals(editedPostId));
							});
				});
	}

	private static final MessageDigest MD5;

	static {
		try {
			MD5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ignored) {
			throw new AssertionError("Apparently, MD5 algorithm does not exist");
		}
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
		if (email == null) {
			return "https://gravatar.com/avatar?d=mp";
		}
		return "https://gravatar.com/avatar/" + md5(email) + "?d=identicon";
	}

	private static Map<String, Set<AttachmentView>> convert(Map<String, Attachment> attachments) {
		return attachments.entrySet().stream()
				.sorted(ATTACHMENT_COMPARATOR)
				.collect(toMultimap(entry -> entry.getValue().getAttachmentType().toString().toLowerCase(),
						entry -> new AttachmentView(entry.getValue().getFilename(), entry.getKey())));
	}

}
