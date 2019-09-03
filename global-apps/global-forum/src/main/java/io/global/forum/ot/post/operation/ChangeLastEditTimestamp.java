package io.global.forum.ot.post.operation;

import io.global.forum.pojo.Post;

import java.util.Map;

public final class ChangeLastEditTimestamp implements ThreadOperation {
	private final String postId;
	private final long prevTimestamp;
	private final long nextTimestamp;

	public ChangeLastEditTimestamp(String postId, long prevTimestamp, long nextTimestamp) {
		this.postId = postId;
		this.prevTimestamp = prevTimestamp;
		this.nextTimestamp = nextTimestamp;
	}

	@Override
	public void apply(Map<String, Post> posts) {
		posts.get(postId).setLastEditTimestamp(nextTimestamp);
	}

	public String getPostId() {
		return postId;
	}

	public long getPrevTimestamp() {
		return prevTimestamp;
	}

	public long getNextTimestamp() {
		return nextTimestamp;
	}

	@Override
	public String toString() {
		return "ChangeLastEditTimestamp{" +
				"postId=" + postId +
				", prevTimestamp=" + prevTimestamp +
				", nextTimestamp=" + nextTimestamp +
				'}';
	}
}
