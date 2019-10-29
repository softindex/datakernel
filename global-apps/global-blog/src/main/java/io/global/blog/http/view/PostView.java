package io.global.blog.http.view;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PostView {
	private final Map<String, Set<String>> attachments;
	private final String lastEditTimestamp;
	private final String initialTimestamp;
	private final boolean deletedVisible;
	private List<PostView> children;
	private final String avatarUrl;
	private final boolean editable;
	private final boolean deepest;
	private final String authorId;
	private final String postId;
	private final String author;
	private String content;

	@Nullable private final String deletedBy;
	@Nullable private final PostView parent;
	@Nullable private String renderedContent;

	public PostView(String postId, String content, boolean deepest, String initialTimestamp, String lastEditTimestamp,
					@Nullable String author, String authorId, String avatarUrl, Map<String, Set<String>> attachments,
					@Nullable String deletedBy, @Nullable PostView parent, boolean editable, boolean deletedVisible) {
		this.postId = postId;
		this.content = content;
		this.deepest = deepest;
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

	public boolean isDeepest() {
		return deepest;
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
}
