package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.global.comm.pojo.Attachment;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public interface ThreadDao {
	StacklessException POST_NOT_FOUND = new StacklessException(ThreadDao.class, "Post with given id not found");
	StacklessException ATTACHMENT_NOT_FOUND = new StacklessException(ThreadDao.class, "Attachment not found");
	StacklessException ROOT_ALREADY_PRESENT_EXCEPTION = new StacklessException(ThreadDao.class, "Root post has already been added");

	Promise<ThreadMetadata> getThreadMetadata();

	default Promise<Void> addRootPost(UserId author, String content, Map<String, Attachment> attachments) {
		return addPost(author, null, content, attachments).toVoid();
	}

	Promise<String> addPost(UserId author, @Nullable String parentId, @Nullable String content, Map<String, Attachment> attachments);

	Promise<Post> getPost(String postId);

	Promise<Void> removePost(UserId user, String postId);

	Promise<Void> restorePost(String postId);

	Promise<Void> updatePost(String postId, @Nullable String newContent, Map<String, Attachment> newAttachments, Set<String> toBeRemoved);

	Promise<Attachment> getAttachment(String postId, String globalFsId);

	Promise<Void> like(UserId user, String postId);

	Promise<Void> dislike(UserId user, String postId);

	Promise<Void> removeLikeOrDislike(UserId user, String postId);

	Promise<Map<String, Post>> listPosts();

	Promise<AttachmentUploader> uploadAttachment();

	default Promise<Void> deleteAttachments(Set<String> globalFsIds) {
		return Promises.all(globalFsIds.stream().map(this::deleteAttachment));
	}

	Promise<Void> deleteAttachment(String globalFsId);

	Promise<Long> attachmentSize(String globalFsId);

	Promise<ChannelSupplier<ByteBuf>> loadAttachment(String globalFsId, long offset, long limit);

	default Promise<ChannelSupplier<ByteBuf>> loadAttachment(String globalFsId) {
		return loadAttachment(globalFsId, 0, -1);
	}

	class AttachmentUploader {
		private final String globalFsId;
		private final ChannelConsumer<ByteBuf> uploader;

		public AttachmentUploader(String globalFsId, ChannelConsumer<ByteBuf> uploader) {
			this.globalFsId = globalFsId;
			this.uploader = uploader;
		}

		public String getGlobalFsId() {
			return globalFsId;
		}

		public ChannelConsumer<ByteBuf> getUploader() {
			return uploader;
		}
	}
}
