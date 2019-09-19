package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.ot.OTStateManager;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;
import io.global.comm.ot.post.ThreadOTState;
import io.global.comm.ot.post.operation.*;
import io.global.comm.pojo.Attachment;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserId;
import io.global.comm.util.Utils;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.global.comm.ot.post.operation.PostChangesOperation.attachmentsToOps;
import static io.global.comm.ot.post.operation.PostChangesOperation.rating;
import static io.global.comm.util.Utils.generateId;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class ThreadDaoImpl implements ThreadDao {
	private final CommDao parent;
	private final String threadId;
	private final OTStateManager<CommitId, ThreadOperation> stateManager;
	private final Map<String, Post> postsView;

	private final FsClient attachmentFs;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public ThreadDaoImpl(CommDao parent, String threadId, OTStateManager<CommitId, ThreadOperation> stateManager, FsClient attachmentFs) {
		this.parent = parent;
		this.threadId = threadId;
		this.stateManager = stateManager;
		this.attachmentFs = attachmentFs;
		this.postsView = ((ThreadOTState) stateManager.getState()).getPostsView();
	}

	@Override
	public Promise<ThreadMetadata> getThreadMetadata() {
		return parent.getThreads().map(threads -> threads.get(threadId));
	}

	@Override
	public Promise<String> addPost(UserId author, String parentId, String content, Map<String, Attachment> attachments) {
		long initialTimestamp = now.currentTimeMillis();
		return Promise.complete()
				.then($ -> {
					if (parentId == null && !postsView.isEmpty()) {
						return Promise.ofException(ROOT_ALREADY_PRESENT_EXCEPTION);
					}
					String postId;
					if (parentId == null) {
						postId = "root";
					} else {
						do {
							postId = Utils.generateId();
						} while (postsView.containsKey(postId));
					}
					stateManager.add(AddPost.addPost(postId, parentId, author, initialTimestamp));
					stateManager.add(PostChangesOperation.forNewPost(postId, content, attachments, initialTimestamp));
					String finalPostId = postId;
					return stateManager.sync().map($2 -> finalPostId);
				});
	}

	@Override
	public Promise<Post> getPost(String postId) {
		Post post = postsView.get(postId);
		return post == null ? Promise.ofException(POST_NOT_FOUND) : Promise.of(post);
	}

	@Override
	public Promise<Void> removePost(UserId user, String postId) {
		long lastEditTimestamp = now.currentTimeMillis();
		return getPost(postId)
				.then(post -> {
					long prevLastEditTimestamp = post.getLastEditTimestamp();
					stateManager.add(PostChangesOperation.delete(postId, user, prevLastEditTimestamp, lastEditTimestamp));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Void> restorePost(String postId) {
		long lastEditTimestamp = now.currentTimeMillis();
		return getPost(postId)
				.then(post -> {
					long prevLastEditTimestamp = post.getLastEditTimestamp();
					stateManager.add(PostChangesOperation.restore(postId, post.getDeletedBy(), prevLastEditTimestamp, lastEditTimestamp));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Void> updatePost(String postId, @Nullable String newContent, Map<String, Attachment> newAttachments, Set<String> toBeRemoved) {
		long lastEditTimestamp = now.currentTimeMillis();
		return getPost(postId)
				.then(post -> {
					long prevLastEditTimestamp = post.getLastEditTimestamp();
					List<ChangeContent> changeContent = new ArrayList<>();
					List<ChangeAttachments> changeAttachments = new ArrayList<>();
					String prevContent = post.getContent();
					if (newContent != null && !newContent.equals(prevContent)) {
						changeContent.add(new ChangeContent(postId, prevContent, newContent, lastEditTimestamp));
					}
					if (!newAttachments.isEmpty()) {
						changeAttachments.addAll(attachmentsToOps(postId, newAttachments, lastEditTimestamp, false));
					}
					if (!toBeRemoved.isEmpty()) {
						Map<String, Attachment> existingAttachments = post.getAttachments();
						Map<String, Attachment> toBeRemovedMap = toBeRemoved.stream()
								.filter(existingAttachments::containsKey)
								.collect(Collectors.toMap(Function.identity(), existingAttachments::get));
						changeAttachments.addAll(attachmentsToOps(postId, toBeRemovedMap, lastEditTimestamp, true));
					}
					if (changeContent.isEmpty() && changeAttachments.isEmpty()) {
						return Promise.complete();
					}

					ChangeLastEditTimestamp changeTimestamp = new ChangeLastEditTimestamp(postId, prevLastEditTimestamp, lastEditTimestamp);
					List<ChangeLastEditTimestamp> changeTimestamps = singletonList(changeTimestamp);
					stateManager.add(new PostChangesOperation(changeContent, changeAttachments, emptyList(), emptyList(), changeTimestamps));
					return stateManager.sync();
				});

	}

	@Override
	public Promise<Attachment> getAttachment(String postId, String globalFsId) {
		return getPost(postId)
				.then(post -> {
					Attachment attachment = post.getAttachments().get(globalFsId);
					if (attachment == null) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(attachment);
				});
	}

	@Override
	public Promise<Void> like(UserId user, String postId) {
		return getPost(postId)
				.then(post -> {
					if (post.getLikes().contains(user)) {
						return Promise.complete();
					}
					Boolean previousValue = post.getDislikes().contains(user) ? Boolean.FALSE : null;
					stateManager.add(rating(postId, user, previousValue, Boolean.TRUE));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Void> dislike(UserId user, String postId) {
		return getPost(postId)
				.then(post -> {
					if (post.getDislikes().contains(user)) {
						return Promise.complete();
					}
					Boolean previousValue = post.getLikes().contains(user) ? Boolean.TRUE : null;
					stateManager.add(rating(postId, user, previousValue, Boolean.FALSE));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Void> removeLikeOrDislike(UserId user, String postId) {
		return getPost(postId)
				.then(post -> {
					Boolean previousValue;
					if (post.getDislikes().contains(user)) {
						previousValue = Boolean.FALSE;
					} else if (post.getLikes().contains(user)) {
						previousValue = Boolean.TRUE;
					} else {
						return Promise.complete();
					}
					stateManager.add(rating(postId, user, previousValue, null));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Map<String, Post>> listPosts() {
		return Promise.of(postsView);
	}

	@Override
	public Promise<AttachmentUploader> uploadAttachment() {
		return Promises.until(() -> Promise.of(generateId()),
				globalFsId -> attachmentFs.getMetadata(globalFsId)
						.map(Objects::isNull))
				.then(globalFsId ->
						attachmentFs.upload(globalFsId)
								.map(uploader -> new AttachmentUploader(globalFsId, uploader)));
	}

	@Override
	public Promise<Void> deleteAttachment(String globalFsId) {
		return attachmentFs.delete(globalFsId);
	}

	@Override
	public Promise<Long> attachmentSize(String globalFsId) {
		return attachmentFs.getMetadata(globalFsId)
				.then(metadata -> {
					if (metadata == null) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(metadata.getSize());
				});
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> loadAttachment(String globalFsId, long offset, long limit) {
		return attachmentFs.download(globalFsId, offset, limit)
				.thenEx((value, e) -> {
					if (e == FsClient.FILE_NOT_FOUND) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(value, e);
				});
	}
}
