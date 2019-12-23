package io.global.blog.http.view;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PostView {
	private final String postId;
	private final String initialTimestamp;
	private final String lastEditTimestamp;
	private final String author;
	private final String authorId;
	private final String avatarUrl;
	private final boolean deletedVisible;
	private final boolean editable;
	private final boolean deepest;
	private long initialTimestampValue;
	private final Map<String, Set<String>> attachments;
	private List<PostView> children;
	private String content;
	@Nullable private String renderedContent;
	@Nullable private final PostView parent;
	@Nullable private final String deletedBy;

	public PostView(
			String postId, String content, boolean deepest, String initialTimestamp, String lastEditTimestamp,
			@Nullable String author, String authorId, String avatarUrl,
			Map<String, Set<String>> attachments,
			@Nullable String deletedBy, @Nullable PostView parent,
			boolean editable, boolean deletedVisible, long initialTimestampValue) {
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
		this.initialTimestampValue = initialTimestampValue;
	}

	public String getPostId() {
		return postId;
	}

	@Nullable
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

	public int numberOfAllChildren() {
		return getNumber(children) + 1;
	}

	private int getNumber(List<PostView> children) {
		int number = 0;
		for (PostView child : children) {
			number += getNumber(child.getChildren());
			number++;
		}
		return number;
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

	public void withRenderedContent(String renderedContent) {
		if (this.renderedContent == null) {
			this.renderedContent = renderedContent;
		}
	}

	public void withContent(String content) {
		this.content = content;
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

	public long getInitialTimestampValue() {
		return initialTimestampValue;
	}
}
