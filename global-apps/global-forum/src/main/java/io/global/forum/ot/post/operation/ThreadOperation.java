package io.global.forum.ot.post.operation;

import io.global.forum.pojo.Post;

import java.util.Map;

public interface ThreadOperation {
	void apply(Map<String, Post> posts);
}
