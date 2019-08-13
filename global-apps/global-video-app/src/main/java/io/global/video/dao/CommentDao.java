package io.global.video.dao;

import io.datakernel.async.Promise;
import io.global.video.pojo.Comment;
import io.global.video.pojo.UserId;

import java.util.Map;
import java.util.Set;

public interface CommentDao {
	Promise<Void> addComment(UserId author, String content);

	Promise<Map<Long, Comment>> listComments();

	Promise<Void> removeComments(Set<Long> commentIds);
}
