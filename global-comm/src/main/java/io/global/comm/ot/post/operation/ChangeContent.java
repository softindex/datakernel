package io.global.comm.ot.post.operation;

import io.global.comm.pojo.Post;
import io.global.ot.value.ChangeValue;

import java.util.Map;

import static io.datakernel.util.StringFormatUtils.limit;

public final class ChangeContent implements ThreadOperation {
	private final String postId;
	private final ChangeValue<String> changeContent;

	public ChangeContent(String postId, ChangeValue<String> changeContent) {
		this.postId = postId;
		this.changeContent = changeContent;
	}

	public ChangeContent(String postId, String prev, String next, long timestamp) {
		this(postId, ChangeValue.of(prev, next, timestamp));
	}

	@Override
	public void apply(Map<String, Post> posts) {
		Post post = posts.get(postId);
		post.setContent(changeContent.getNext());
	}

	public String getPostId() {
		return postId;
	}

	public ChangeValue<String> getChangeContent() {
		return changeContent;
	}

	public String getPrev() {
		return changeContent.getPrev();
	}

	public String getNext() {
		return changeContent.getNext();
	}

	public long getTimestamp() {
		return changeContent.getTimestamp();
	}

	@Override
	public String toString() {
		return "ChangeContent{" +
				"postId=" + postId +
				", prev=" + limit(changeContent.getPrev(), 20) +
				", next=" + limit(changeContent.getNext(), 20) +
				", timestamp=" + changeContent.getTimestamp() +
				'}';
	}
}
