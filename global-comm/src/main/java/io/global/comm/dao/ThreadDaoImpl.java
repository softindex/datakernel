package io.global.comm.dao;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.comm.ot.post.ThreadOTState;
import io.global.comm.ot.post.operation.*;
import io.global.comm.pojo.AttachmentType;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserId;
import io.global.comm.util.Utils;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.global.comm.ot.post.operation.PostChangesOperation.attachmentsToOps;
import static io.global.comm.ot.post.operation.PostChangesOperation.rating;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;

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
	public Promise<String> generatePostId() {
		String postId;
		do {
			postId = Utils.generateId();
		} while (postsView.containsKey(postId));
		return Promise.of(postId);
	}

	@Override
	public Promise<ThreadMetadata> getThreadMetadata() {
		return parent.getThreads().map(threads -> threads.get(threadId));
	}

	@Override
	public Promise<Void> addPost(UserId author, @Nullable String parentId, String postId, String content, Map<String, AttachmentType> attachments) {
		long initialTimestamp = now.currentTimeMillis();
		return Promise.complete()
				.then($ -> {
					if (parentId == null && !postsView.isEmpty()) {
						return Promise.ofException(ROOT_ALREADY_PRESENT_EXCEPTION);
					}
					stateManager.add(AddPost.addPost(postId, parentId, author, initialTimestamp));
					stateManager.add(PostChangesOperation.forNewPost(postId, content, attachments, initialTimestamp));
					return stateManager.sync();
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
	public Promise<Void> updatePost(String postId, @Nullable String newContent, Map<String, AttachmentType> newAttachments, Set<String> toBeRemoved) {
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
						Map<String, AttachmentType> existingAttachments = post.getAttachments();
						Map<String, AttachmentType> toBeRemovedMap = toBeRemoved.stream()
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
	public Promise<Set<String>> listAttachments(String postId) {
		int prefixLength = postId.length() + 1;
		return attachmentFs.list(postId + "/*")
				.map(list -> list.stream()
						.map(m -> m.getName().substring(prefixLength))
						.collect(toSet()));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> uploadAttachment(String postId, String filename) {
		return attachmentFs.upload(postId + "/" + filename, 0, System.currentTimeMillis());
	}

	@Override
	public Promise<Void> deleteAttachment(String postId, String filename) {
		return attachmentFs.delete(postId + "/" + filename, System.currentTimeMillis());
	}

	@Override
	public Promise<Long> attachmentSize(String postId, String filename) {
		return attachmentFs.getMetadata(postId + "/" + filename)
				.then(metadata -> {
					if (metadata == null) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(metadata.getSize());
				});
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> loadAttachment(String postId, String filename, long offset, long limit) {
		return attachmentFs.download(postId + "/" + filename, offset, limit)
				.thenEx((value, e) -> {
					if (e == FsClient.FILE_NOT_FOUND) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(value, e);
				});
	}
}
