package io.global.comm.ot.post.operation;

import io.global.comm.pojo.Post;

import java.util.Map;

public interface ThreadOperation {
	void apply(Map<String, Post> posts);
}
