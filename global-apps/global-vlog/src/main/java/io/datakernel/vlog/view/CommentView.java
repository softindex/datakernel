package io.datakernel.vlog.view;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public final class CommentView {
	private final String lastEditTimestamp;
	private final String initialTimestamp;
	private final boolean deletedVisible;
	private final String avatarUrl;
	private final boolean editable;
	private final boolean deepest;
	private final String authorId;
	private final String id;
	private final String author;
	private final String content;
	private List<CommentView> children;

	@Nullable private final String deletedBy;
	@Nullable private final CommentView parent;

	public CommentView(String lastEditTimestamp, String initialTimestamp, boolean deletedVisible, String avatarUrl,
					   boolean editable, boolean deepest, String id, String content, String authorId, @Nullable String author,
					   @Nullable String deletedBy, @Nullable CommentView parent) {
		this.lastEditTimestamp = lastEditTimestamp;
		this.initialTimestamp = initialTimestamp;
		this.deletedVisible = deletedVisible;
		this.avatarUrl = avatarUrl;
		this.editable = editable;
		this.deepest = deepest;
		this.authorId = authorId;
		this.id = id;
		this.author = author;
		this.content = content;
		this.deletedBy = deletedBy;
		this.parent = parent;
	}

	@Nullable
	public CommentView getParent() {
		return parent;
	}

	public String getLastEditTimestamp() {
		return lastEditTimestamp;
	}

	public String getInitialTimestamp() {
		return initialTimestamp;
	}

	public boolean isDeletedVisible() {
		return deletedVisible;
	}

	public String getAvatarUrl() {
		return avatarUrl;
	}

	public boolean isEditable() {
		return editable;
	}

	public boolean isDeepest() {
		return deepest;
	}

	public String getAuthorId() {
		return authorId;
	}

	public String getId() {
		return id;
	}

	public String getAuthor() {
		return author;
	}

	public String getContent() {
		return content;
	}

	public List<CommentView> getChildren() {
		return children;
	}

	public int numberOfAllChildren() {
		return getNumber(children) + 1;
	}

	private int getNumber(List<CommentView> children) {
		int result = 0;
		for (CommentView child : children) {
			result += getNumber(child.getChildren());
			result++;
		}
		return result;
	}


	public CommentView setChildren(List<CommentView> children) {
		this.children = children;
		return this;
	}

	@Nullable
	public String getDeletedBy() {
		return deletedBy;
	}

	public CommentView withChildren(List<CommentView> children) {
		this.children = children;
		return this;
	}
}
