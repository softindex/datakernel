package io.global.comm.ot.post;

import io.datakernel.ot.OTState;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.Post;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ThreadOTState implements OTState<ThreadOperation> {
	private Map<String, Post> posts = new HashMap<>();

	@Override
	public void init() {
		posts.clear();
	}

	@Override
	public void apply(ThreadOperation op) {
		op.apply(posts);
	}

	public Map<String, Post> getPostsView() {
		return Collections.unmodifiableMap(posts);
	}
}
