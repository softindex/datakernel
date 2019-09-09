package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.common.CryptoUtils;
import io.global.forum.dao.ForumDao;
import io.global.forum.pojo.*;
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static io.global.forum.pojo.ForumPrivilege.*;
import static java.util.stream.Collectors.toList;

public final class PostView {
	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final Comparator<Map.Entry<String, Attachment>> ATTACHMENT_COMPARATOR = Comparator.comparing(a -> a.getValue().getFilename());
	private static final Comparator<Post> POST_COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);

	private final String postId;
	private final String author;
	private final String content;
	private final String initialTimestamp;
	private final String lastEditTimestamp;

	private final List<PostView> children;
	private final List<AttachmentView> attachments;

	@Nullable
	private final String emailMd5;

	@Nullable
	private final String deletedBy;

	private final boolean editable;
	private final boolean deletedVisible;

	private final boolean editedNow;

	public PostView(String postId, String author, String content, String initialTimestamp, String lastEditTimestamp,
			List<PostView> children, List<AttachmentView> attachments, String emailMd5, @Nullable String deletedBy,
			boolean editable, boolean deletedVisible, boolean editedNow) {
		this.postId = postId;
		this.author = author;
		this.content = content;
		this.initialTimestamp = initialTimestamp;
		this.lastEditTimestamp = lastEditTimestamp;
		this.children = children;
		this.attachments = attachments;
		this.emailMd5 = emailMd5;
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

	public List<AttachmentView> getAttachments() {
		return attachments;
	}

	@Nullable
	public String getDeletedBy() {
		return deletedBy;
	}

	@Nullable
	public String getEmailMd5() {
		return emailMd5;
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
	public static Promise<PostView> from(ForumDao forumDao, Post post, @Nullable UserId currentUser, @Nullable String editedPostId) {
		return Promises.toList(post.getChildren().stream().sorted(POST_COMPARATOR).map(p -> from(forumDao, p, currentUser, editedPostId)))
				.then(children -> {
					UserId deleterId = post.getDeletedBy();
					Promise<@Nullable UserData> authorPromise = forumDao.getUser(post.getAuthor());
					Promise<@Nullable UserData> deleterPromise = deleterId != null ? forumDao.getUser(deleterId) : Promise.of(null);
					Promise<@Nullable UserData> currentPromise = currentUser != null ? forumDao.getUser(currentUser) : Promise.of(null);
					return Promises.toTuple(authorPromise, deleterPromise, currentPromise)
							.map(users -> {
								UserData author = users.getValue1();
								UserData deleter = users.getValue2();
								UserData current = users.getValue3();
								UserRole role = current != null ? current.getRole() : UserRole.NONE;

								boolean own = post.getAuthor().equals(currentUser);
								return new PostView(
										post.getId(),
										author != null ? author.getUsername() : null,
										post.getContent(),
										format(post.getInitialTimestamp()),
										format(post.getLastEditTimestamp()),
										children,
										post.getAttachments().entrySet().stream()
												.sorted(ATTACHMENT_COMPARATOR)
												.map(AttachmentView::from)
												.collect(toList()),
										author != null ? md5(author.getEmail()) : null,
										deleter != null ? deleter.getUsername() : null,
										role.has(EDIT_ANY_POST) || own && role.has(EDIT_OWN_POST) && (deleterId == null || post.getAuthor().equals(deleterId)),
										role.has(SEE_ANY_DELETED_POSTS) || own && role.has(SEE_OWN_DELETED_POSTS),
										post.getId().equals(editedPostId));
							});
				});
	}

	private static String md5(String str) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new AssertionError(e);
		}
		md.update(str.getBytes());
		return CryptoUtils.toHexString(md.digest());
	}
}
