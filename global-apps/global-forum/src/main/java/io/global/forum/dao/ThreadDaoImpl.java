package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTStateManager;
import io.datakernel.time.CurrentTimeProvider;
import io.global.forum.Utils;
import io.global.forum.ot.post.ThreadOTState;
import io.global.forum.ot.post.operation.*;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.global.forum.ot.post.operation.PostChangesOperation.attachmentsToOps;
import static io.global.forum.ot.post.operation.PostChangesOperation.rating;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class ThreadDaoImpl implements ThreadDao {
	private final OTStateManager<CommitId, ThreadOperation> stateManager;
	private final Map<Long, Post> postsView;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public ThreadDaoImpl(OTStateManager<CommitId, ThreadOperation> stateManager) {
		this.stateManager = stateManager;
		this.postsView = ((ThreadOTState) stateManager.getState()).getPostsView();
	}

	@Override
	public Promise<Void> addPost(UserId author, @Nullable Long parentId, String content, Map<String, Attachment> attachments) {
		long initialTimestamp = now.currentTimeMillis();
		return Promise.complete()
				.then($ -> {
					if (parentId == null && !postsView.isEmpty()) {
						return Promise.ofException(ROOT_ALREADY_PRESENT_EXCEPTION);
					}
					Long postId;
					if (parentId == null) {
						postId = 0L;
					} else {
						do {
							postId = Utils.generateId();
						} while (!postsView.containsKey(postId));
					}
					stateManager.add(AddPost.addPost(postId, parentId, author, initialTimestamp));
					stateManager.add(PostChangesOperation.forNewPost(postId, content, attachments, initialTimestamp));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Post> getPost(Long postId) {
		Post post = postsView.get(postId);
		return post == null ? Promise.ofException(POST_NOT_FOUND) : Promise.of(post);
	}

	@Override
	public Promise<Void> removePost(UserId user, Long postId) {
		long lastEditTimestamp = now.currentTimeMillis();
		return getPost(postId)
				.then(post -> {
					long prevLastEditTimestamp = post.getLastEditTimestamp();
					stateManager.add(PostChangesOperation.delete(postId, user, prevLastEditTimestamp, lastEditTimestamp));
					return stateManager.sync();
				});
	}

	@Override
	public Promise<Void> updatePost(Long postId, @Nullable String newContent, Map<String, Attachment> newAttachments, Set<String> toBeRemoved) {
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
	public Promise<Attachment> getAttachment(Long postId, String globalFsId) {
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
	public Promise<Void> like(UserId user, Long postId) {
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
	public Promise<Void> dislike(UserId user, Long postId) {
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
	public Promise<Void> removeLikeOrDislike(UserId user, Long postId) {
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
	public Promise<Map<Long, Post>> listPosts() {
		return Promise.of(postsView);
	}
}
