package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;

import java.util.Map;
import java.util.Set;

public interface PostDao {
	Promise<Void> addComment(UserId author, String content);

	Promise<Map<Long, Post>> listComments();

	Promise<Void> removeComments(Set<Long> commentIds);
}
