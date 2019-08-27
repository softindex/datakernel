package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.exception.StacklessException;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public interface ThreadDao {
	StacklessException POST_NOT_FOUND = new StacklessException(ThreadDao.class, "Post with given id not found");
	StacklessException ATTACHMENT_NOT_FOUND = new StacklessException(ThreadDao.class, "Attachment not found");
	StacklessException ROOT_ALREADY_PRESENT_EXCEPTION = new StacklessException(ThreadDao.class, "Root post has already been added");

	default Promise<Void> addRootPost(UserId author, String content, Map<String, Attachment> attachments) {
		return addPost(author, null, content, attachments);
	}

	Promise<Void> addPost(UserId author, @Nullable Long parentId, @Nullable String content, Map<String, Attachment> attachments);

	Promise<Post> getPost(Long postId);

	Promise<Void> removePost(UserId user, Long postId);

	Promise<Void> updatePost(Long postId, @Nullable String newContent, Map<String, Attachment> newAttachments, Set<String> toBeRemoved);

	Promise<Attachment> getAttachment(Long postId, String globalFsId);

	Promise<Void> like(UserId user, Long postId);

	Promise<Void> dislike(UserId user, Long postId);

	Promise<Void> removeLikeOrDislike(UserId user, Long postId);

	Promise<Map<Long, Post>> listPosts();
}
