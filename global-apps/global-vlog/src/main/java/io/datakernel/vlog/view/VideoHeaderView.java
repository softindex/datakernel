package io.datakernel.vlog.view;

import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static java.lang.Math.min;

public final class VideoHeaderView {
	private static final int DEFAULT_PREVIEW_SIZE = 64;
	public static final String VIDEO_VIEW_HEADER = "header";
	private final Map<String, String> videoResource;
	private final String videoPoster;
	private final String lastEditTimestamp;
	private final String initialTimestamp;
	private final boolean deletedVisible;
	private final boolean editable;
	private final String avatarUrl;
	private final String authorId;
	private final String id;
	private final String author;
	private final String content;
	private final int likes;
	private final int dislikes;
	private final boolean isLikedBy;
	private final boolean isDislikedBy;
	private final boolean isPending;
	@Nullable private final String deletedBy;
	private final long initialTimestampValue;

	public VideoHeaderView(Map<String, String> videoResource, String videoPoster, String lastEditTimestamp,
						   String initialTimestamp, long initialTimestampValue, boolean deletedVisible, boolean editable, String avatarUrl,
						   String id, String content, boolean isPending, String authorId, @Nullable String author, @Nullable String deletedBy, int likes, int dislikes,
						   boolean isLikedBy, boolean isDislikedBy) {
		this.videoResource = videoResource;
		this.videoPoster = videoPoster;
		this.lastEditTimestamp = lastEditTimestamp;
		this.initialTimestamp = initialTimestamp;
		this.deletedVisible = deletedVisible;
		this.initialTimestampValue = initialTimestampValue;
		this.editable = editable;
		this.avatarUrl = avatarUrl;
		this.authorId = authorId;
		this.id = id;
		this.author = author;
		this.content = content;
		this.isPending = isPending;
		this.deletedBy = deletedBy;
		this.likes = likes;
		this.dislikes = dislikes;
		this.isLikedBy = isLikedBy;
		this.isDislikedBy = isDislikedBy;
	}

	public Map<String, String> getVideoResource() {
		return videoResource;
	}

	public String getVideoPoster() {
		return videoPoster;
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

	public boolean isEditable() {
		return editable;
	}

	public String getAvatarUrl() {
		return avatarUrl;
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

	public String getDeletedBy() {
		return deletedBy;
	}

	public int getDislikes() {
		return dislikes;
	}

	public int getLikes() {
		return likes;
	}

	public boolean isLikedBy() {
		return isLikedBy;
	}

	public boolean isDislikedBy() {
		return isDislikedBy;
	}

	public boolean isPending() {
		return isPending;
	}

	public long getInitialTimestampValue() {
		return initialTimestampValue;
	}

	public String previewContent() {
		return content.length() < DEFAULT_PREVIEW_SIZE ? content : content.substring(0, min(content.length(), 32)) + "...";
	}
}
