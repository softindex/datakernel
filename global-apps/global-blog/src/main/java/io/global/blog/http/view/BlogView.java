package io.global.blog.http.view;

import io.global.comm.pojo.ThreadMetadata;

public class BlogView {
	private final String id;
	private final ThreadMetadata meta;
	private final PostView root;
	private final int commentCount;

	public BlogView(String id, ThreadMetadata meta, PostView root, int commentCount) {
		this.id = id;
		this.meta = meta;
		this.root = root;
		this.commentCount = commentCount;
	}

	public String getId() {
		return id;
	}

	public ThreadMetadata getMeta() {
		return meta;
	}

	public PostView getRoot() {
		return root;
	}

	public int getCommentCount() {
		return commentCount;
	}
}
