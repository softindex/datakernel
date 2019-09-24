package io.global.comm.ot.post.operation;

import io.global.comm.pojo.AttachmentType;
import io.global.comm.pojo.Post;

import java.util.Map;

import static io.global.comm.pojo.AttachmentType.DOCUMENT;

public final class ChangeAttachments implements ThreadOperation {
	public static final ChangeAttachments EMPTY = new ChangeAttachments("", "", DOCUMENT, 0, true);

	private final String postId;
	private final String filename;
	private final AttachmentType attachmentType;
	private final long timestamp;
	private final boolean remove;

	public ChangeAttachments(String postId, String filename, AttachmentType attachmentType, long timestamp, boolean remove) {
		this.postId = postId;
		this.filename = filename;
		this.attachmentType = attachmentType;
		this.timestamp = timestamp;
		this.remove = remove;
	}

	@Override
	public void apply(Map<String, Post> posts) {
		Post post = posts.get(postId);
		if (remove) {
			post.removeAttachment(filename);
		} else {
			post.addAttachment(filename, attachmentType);
		}
	}

	public ChangeAttachments invert() {
		return new ChangeAttachments(postId, filename, attachmentType, timestamp, !remove);
	}

	public boolean isInversionFor(ChangeAttachments other) {
		return postId.equals(other.postId) &&
				filename.equals(other.filename) &&
				attachmentType == other.attachmentType &&
				timestamp == other.timestamp &&
				remove != other.remove;
	}

	public String getPostId() {
		return postId;
	}

	public String getFilename() {
		return filename;
	}

	public AttachmentType getAttachmentType() {
		return attachmentType;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isRemove() {
		return remove;
	}

	@Override
	public String toString() {
		return "ChangeAttachments{" +
				"postId=" + postId +
				", filename='" + filename + '\'' +
				", attachmentType=" + attachmentType +
				", timestamp=" + timestamp +
				", remove=" + remove +
				'}';
	}
}
