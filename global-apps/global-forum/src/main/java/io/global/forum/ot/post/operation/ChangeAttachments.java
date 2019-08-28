package io.global.forum.ot.post.operation;

import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;

import java.util.Map;

import static io.global.forum.pojo.AttachmentType.DOCUMENT;

public final class ChangeAttachments implements ThreadOperation {
	public static final ChangeAttachments EMPTY = new ChangeAttachments(0, "", new Attachment(DOCUMENT, ""), 0, true);

	private final long postId;
	private final String globalFsId;
	private final Attachment attachment;
	private final long timestamp;
	private final boolean remove;

	public ChangeAttachments(long postId, String globalFsId, Attachment attachment, long timestamp, boolean remove) {
		this.postId = postId;
		this.globalFsId = globalFsId;
		this.attachment = attachment;
		this.timestamp = timestamp;
		this.remove = remove;
	}

	@Override
	public void apply(Map<Long, Post> posts) {
		Post post = posts.get(postId);
		if (remove) {
			post.removeAttachment(globalFsId);
		} else {
			post.addAttachment(globalFsId, attachment);
		}
	}

	public ChangeAttachments invert() {
		return new ChangeAttachments(postId, globalFsId, attachment, timestamp, !remove);
	}

	public boolean isInversionFor(ChangeAttachments other) {
		return postId == other.postId &&
				globalFsId.equals(other.globalFsId) &&
				attachment.equals(other.attachment) &&
				timestamp == other.timestamp &&
				remove != other.remove;
	}

	public long getPostId() {
		return postId;
	}

	public String getGlobalFsId() {
		return globalFsId;
	}

	public Attachment getAttachment() {
		return attachment;
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
				", globalFsId='" + globalFsId + '\'' +
				", attachment=" + attachment +
				", timestamp=" + timestamp +
				", remove=" + remove +
				'}';
	}
}
