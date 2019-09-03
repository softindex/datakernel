package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.forum.dao.ForumDao;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public final class PostView {
	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final Comparator<Map.Entry<String, Attachment>> ATTACHMENT_COMPARATOR = Comparator.comparing(a -> a.getValue().getFilename());

	private final String postId;
	private final String author;
	private final String content;
	private final String initialTimestamp;
	private final String lastEditTimestamp;

	private final List<PostView> children;
	private final List<AttachmentView> attachments;

	@Nullable
	private final String deletedBy;

	public PostView(String postId, String author, String content, String initialTimestamp, String lastEditTimestamp, List<PostView> children, List<AttachmentView> attachments, @Nullable String deletedBy) {
		this.postId = postId;
		this.author = author;
		this.content = content;
		this.initialTimestamp = initialTimestamp;
		this.lastEditTimestamp = lastEditTimestamp;
		this.children = children;
		this.attachments = attachments;
		this.deletedBy = deletedBy;
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

	private static String format(long timestamp) {
		return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	// TODO anton: add recursion hard stop condition (like >100 child depth) and proper view/pagination
	public static Promise<PostView> from(ForumDao forumDao, Post post) {
		return Promises.toList(post.getChildren().stream().map(p -> from(forumDao, p)))
				.then(children -> {
					UserId deleter = post.getDeletedBy();
					Promise<String> username = forumDao.getUser(post.getAuthor()).map(u -> u != null ? u.getUsername() : "ghost");
					Promise<String> deleterName = deleter != null ? forumDao.getUser(deleter).map(u -> u != null ? u.getUsername() : "ghost") : Promise.of(null);
					return Promises.toTuple(username, deleterName)
							.map(names ->
									new PostView(
											post.getId(),
											names.getValue1(),
											post.getContent(),
											format(post.getInitialTimestamp()),
											format(post.getLastEditTimestamp()),
											children,
											post.getAttachments().entrySet().stream()
													.sorted(ATTACHMENT_COMPARATOR)
													.map(AttachmentView::from)
													.collect(toList()),
											names.getValue2()));
				});
	}
}
