package io.global.forum.ot.post;

import io.datakernel.ot.OTState;
import io.global.forum.ot.post.operation.ThreadOperation;
import io.global.forum.pojo.Post;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ThreadOTState implements OTState<ThreadOperation> {
	private Map<Long, Post> posts = new HashMap<>();

	@Override
	public void init() {
		posts.clear();
	}

	@Override
	public void apply(ThreadOperation op) {
		op.apply(posts);
	}

	public Map<Long, Post> getPostsView() {
		return Collections.unmodifiableMap(posts);
	}
}
