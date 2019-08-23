package io.global.forum.ot.post.operation;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.forum.pojo.Post;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;

public final class ChangeLastEditTimestamp implements PostOperation {
	public static final StructuredCodec<ChangeLastEditTimestamp> CODEC = StructuredCodecs.tuple(ChangeLastEditTimestamp::new,
			ChangeLastEditTimestamp::getPostId, LONG_CODEC,
			ChangeLastEditTimestamp::getPrevTimestamp, LONG_CODEC,
			ChangeLastEditTimestamp::getNextTimestamp, LONG_CODEC);

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
