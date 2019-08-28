package io.global.forum.ot.post.operation;

import io.global.forum.pojo.Post;

import java.util.Map;

public final class ChangeLastEditTimestamp implements ThreadOperation {
	private final long postId;
	private final long prevTimestamp;
	private final long nextTimestamp;

	public ChangeLastEditTimestamp(long postId, long prevTimestamp, long nextTimestamp) {
		this.postId = postId;
		this.prevTimestamp = prevTimestamp;
		this.nextTimestamp = nextTimestamp;
	}

	@Override
	public void apply(Map<Long, Post> posts) {
		posts.get(postId).setLastEditTimestamp(nextTimestamp);
	}

	public long getPostId() {
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
