package io.global.comm.dao;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.pojo.AttachmentType;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.ThreadMetadata;
import io.global.ot.session.UserId;
import io.global.comm.pojo.*;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public interface ThreadDao {
	StacklessException POST_NOT_FOUND = new StacklessException(ThreadDao.class, "Post with given id not found");
	StacklessException ATTACHMENT_NOT_FOUND = new StacklessException(ThreadDao.class, "Attachment not found");
	StacklessException ROOT_ALREADY_PRESENT_EXCEPTION = new StacklessException(ThreadDao.class, "Root post has already been added");

	Promise<ThreadMetadata> getThreadMetadata();

	Promise<String> generatePostId();

	default Promise<Void> addRootPost(UserId author, String content, Map<String, AttachmentType> attachments) {
		return addPost(author, null, "root", content, attachments);
	}

	default Promise<Void> addRootPost(UserId author, String content, String postId, Map<String, AttachmentType> attachments) {
		return addPost(author, null, postId, content, attachments);
	}

	Promise<Void> addPost(UserId author, @Nullable String parentId, String postId, @Nullable String content, Map<String, AttachmentType> attachments);

	Promise<Post> getPost(String postId);

	Promise<Void> removePost(UserId user, String postId);

	Promise<Void> restorePost(String postId);

	Promise<Void> updatePost(String postId, @Nullable String newContent, Map<String, AttachmentType> newAttachments, Set<String> toBeRemoved);

	Promise<Void> updateRating(UserId user, String postId, @Nullable Rating rating);

	Promise<Map<String, Post>> listPosts();

	Promise<Set<String>> listAttachments(String postId);

	Promise<ChannelConsumer<ByteBuf>> uploadAttachment(String postId, String filename);

	default Promise<Void> deleteAttachments(String postId, Set<String> filenames) {
		return Promises.all(filenames.stream().map(filename -> deleteAttachment(postId, filename)));
	}

	Promise<Void> deleteAttachment(String postId, String filename);

	Promise<Long> attachmentSize(String postId, String filename);

	Promise<ChannelSupplier<ByteBuf>> loadAttachment(String postId, String filename, long offset, long limit);

	default Promise<ChannelSupplier<ByteBuf>> loadAttachment(String postId, String filename) {
		return loadAttachment(postId, filename, 0, -1);
	}
}
