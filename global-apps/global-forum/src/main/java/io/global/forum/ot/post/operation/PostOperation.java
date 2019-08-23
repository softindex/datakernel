package io.global.forum.ot.post.operation;

import io.global.forum.pojo.Post;

import java.util.Map;

public interface PostOperation {
	void apply(Map<Long, Post> posts);
}
