package io.datakernel.vlog.util;

import io.datakernel.common.Preconditions;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.vlog.view.CommentView;
import io.datakernel.vlog.view.VideoHeaderView;
import io.datakernel.vlog.view.VideoView;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.*;
import io.global.common.CryptoUtils;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Collections.emptyList;

public final class ViewEntityUtil {
	private static final MessageDigest MD5;
	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final Comparator<Post> COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);

	static {
		try {
			MD5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ignored) {
			throw new AssertionError("Apparently, MD5 algorithm does not exist");
		}
	}

	public static Promise<VideoView> videoViewFrom(CommDao commDao, String threadId, @Nullable UserId currentUser, int childrenDepth, List<String> pending) {
		return commDao.getThreadDao(threadId)
				.then(threadDao -> {
					if (threadDao == null || childrenDepth <= 0) {
						return Promise.of(null);
					}
					return threadDao.getPost(VIDEO_VIEW_HEADER)
							.then(videoHeader -> threadDao.getThreadMetadata()
									.then(metadata -> videoHeaderViewFrom(threadId, commDao, videoHeader, currentUser, pending)
											.then(videoHeaderView -> Promises.toList(
													videoHeader.getChildren()
															.stream()
															.map(post -> commentViewFrom(commDao, post, currentUser, childrenDepth - 1, null)))
													.map(commentViews -> new VideoView(threadId, metadata, videoHeaderView, commentViews)))));
				});
	}

	public static Promise<List<VideoView>> videoViewListFrom(CommDao commDao, Map<String, ThreadMetadata> threads,
															 @Nullable UserId currentUser, int page, int size, List<String> pending) {
		long res = page * size;
		long offset = ((int) res != res) ? MAX_VALUE : res;
		return Promises.toList(threads
				.entrySet()
				.stream()
				.skip(offset)
				.limit(size)
				.map(e -> commDao.getThreadDao(e.getKey())
						.then(dao -> {
							if (dao == null) {
								return Promise.of(null);
							}
							return dao.getPost(VIDEO_VIEW_HEADER)
									.thenEx((videoHeader, ex) -> {
										if (ex != null) {
											return Promise.of(null);
										}
										return videoHeaderViewFrom(e.getKey(), commDao, videoHeader, currentUser, pending)
												.map(videoHeaderView -> new VideoView(e.getKey(), e.getValue(), videoHeaderView, emptyList()));
									});
						})))
				.map(list -> list.stream()
						.filter(Objects::nonNull)
						.collect(Collectors.toList()));
	}

	/**
	 * The first param is the author data
	 * The second params is the deleter data
	 * The third params is the current data
	 */
	private static Promise<Tuple3<@Nullable UserData, @Nullable UserData, @Nullable UserData>> userTuple(CommDao commDao, Post post, @Nullable UserId currentUser) {
		UserId deleterId = post.getDeletedBy();
		Promise<@Nullable UserData> authorPromise = commDao.getUsers().get(post.getAuthor());
		Promise<@Nullable UserData> deleterPromise = deleterId != null ? commDao.getUsers().get(deleterId) : Promise.of(null);
		Promise<@Nullable UserData> currentPromise = currentUser != null ? commDao.getUsers().get(currentUser) : Promise.of(null);
		return Promises.toTuple(authorPromise, deleterPromise, currentPromise);
	}

	private static Promise<VideoHeaderView> videoHeaderViewFrom(String threadId, CommDao commDao, Post post, @Nullable UserId currentUser, List<String> pending) {
		return userTuple(commDao, post, currentUser)
				.map(users -> {
					UserData author = users.getValue1();
					UserData deleter = users.getValue2();
					UserData current = users.getValue3();
					return videoHeaderViewFrom(threadId, post, author, deleter, post.getDeletedBy(), current, currentUser, pending);
				});
	}

	private static VideoHeaderView videoHeaderViewFrom(String threadId, Post videoHeader,
													   @Nullable UserData author,
													   @Nullable UserData deleter, @Nullable UserId deleterUser,
													   @Nullable UserData current, @Nullable UserId currentUser,
													   List<String> pending) {
		UserRole role = current != null ? current.getRole() : UserRole.GUEST;
		boolean own = Objects.equals(videoHeader.getAuthor(), currentUser);
		boolean editable = role.isPrivileged() || own && role.isKnown() && (deleterUser == null || videoHeader.getAuthor().equals(deleterUser));
		boolean deletedVisible = role.isPrivileged() || own && role.isKnown();
		return new VideoHeaderView(
				getVideo(videoHeader.getAttachments()),
				getFsId(videoHeader.getAttachments(), AttachmentType.IMAGE),
				format(videoHeader.getLastEditTimestamp()),
				format(videoHeader.getInitialTimestamp()),
				videoHeader.getInitialTimestamp(),
				deletedVisible,
				editable,
				avatarUrl(author),
				videoHeader.getId(),
				videoHeader.getContent(),
				pending.contains(threadId),
				videoHeader.getAuthor().getAuthId(),
				author != null ? author.getUsername() : null,
				deleter != null ? deleter.getUsername() : null,
				videoHeader.getRatings().get(Rating.LIKE).size(),
				videoHeader.getRatings().get(Rating.DISLIKE).size(),
				videoHeader.getRatings().get(Rating.LIKE).contains(currentUser),
				videoHeader.getRatings().get(Rating.DISLIKE).contains(currentUser));
	}

	private static Map<String, String> getVideo(Map<String, AttachmentType> attachments) {
		return attachments
				.entrySet()
				.stream()
				.filter(entry -> entry.getValue() == AttachmentType.VIDEO)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getKey));
	}

	private static Promise<CommentView> commentViewFrom(CommDao commDao, Post post, @Nullable UserId currentUser, int childrenDepth, @Nullable CommentView parent) {
		boolean deepest = childrenDepth == 0;
		UserId deleterId = post.getDeletedBy();
		return userTuple(commDao, post, currentUser)
				.then(users -> {
					UserData author = users.getValue1();
					UserData deleter = users.getValue2();
					UserData current = users.getValue3();
					CommentView root = commentViewFrom(post, parent, false, author, deleter, deleterId, current, currentUser);
					return (deepest ?
							collectAllDeepestChildren(root, post, commDao, currentUser, current) :
							Promises.toList(post
									.getChildren()
									.stream()
									.sorted(COMPARATOR)
									.map(p -> commentViewFrom(commDao, p, currentUser, childrenDepth - 1, root))))
							.map(root::withChildren);
				});
	}

	private static Promise<List<CommentView>> collectAllDeepestChildren(CommentView root, Post post, CommDao commDao, @Nullable UserId currentUser, @Nullable UserData current) {
		ArrayList<CommentView> result = new ArrayList<>();
		return doCollectChildrenPostView(root, post, commDao, currentUser, current, result)
				.map($ -> result);
	}

	private static Promise<Void> doCollectChildrenPostView(CommentView root, Post post, CommDao commDao,
														   @Nullable UserId currentUser, @Nullable UserData current,
														   List<CommentView> resultList) {
		return Promises.all(post.getChildren()
				.stream()
				.map(children -> {
					UserId deleterId = children.getDeletedBy();
					Promise<@Nullable UserData> authorPromise = commDao.getUsers().get(children.getAuthor());
					Promise<@Nullable UserData> deleterPromise = deleterId != null ? commDao.getUsers().get(deleterId) : Promise.of(null);
					return Promises.toTuple(authorPromise, deleterPromise)
							.then(users -> {
								UserData author = users.getValue1();
								UserData deleter = users.getValue2();
								CommentView postView = commentViewFrom(children, root, true, author, deleter, deleterId, current, currentUser)
										.withChildren(emptyList());
								resultList.add(postView);
								return doCollectChildrenPostView(postView, children, commDao, currentUser, current, resultList);
							});
				}));
	}

	private static CommentView commentViewFrom(Post post, @Nullable CommentView parent, boolean deepest,
											   @Nullable UserData author,
											   @Nullable UserData deleter, @Nullable UserId deleterUser,
											   @Nullable UserData current, @Nullable UserId currentUser) {
		UserRole role = current != null ? current.getRole() : UserRole.GUEST;
		boolean own = Objects.equals(post.getAuthor(), currentUser);
		boolean editable = role.isPrivileged() || own && role.isKnown() && (deleterUser == null || post.getAuthor().equals(deleterUser));
		boolean deletedVisible = role.isPrivileged() || own && role.isKnown();
		return new CommentView(
				format(post.getLastEditTimestamp()),
				format(post.getInitialTimestamp()),
				deletedVisible,
				avatarUrl(author),
				editable,
				deepest,
				post.getId(),
				post.getContent(),
				post.getAuthor().getAuthId(),
				author != null ? author.getUsername() : null,
				deleter != null ? deleter.getUsername() : null,
				parent
		);
	}

	@SuppressWarnings("ConstantConditions")
	private static String getFsId(Map<String, AttachmentType> attachments, AttachmentType required) {
		return attachments
				.entrySet()
				.stream()
				.filter(entry -> entry.getValue() == required)
				.findFirst()
				.map(Map.Entry::getKey)
				.orElse(null);
	}


	private static String format(long timestamp) {
		return timestamp == -1 ? "" : Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	@Nullable
	private static String md5(@Nullable String str) {
		Preconditions.checkState(Eventloop.getCurrentEventloop().inEventloopThread());
		if (str == null) {
			return null;
		}
		MD5.update(str.getBytes());
		return CryptoUtils.toHexString(MD5.digest());
	}

	private static String avatarUrl(@Nullable UserData data) {
		if (data == null) {
			return "https://gravatar.com/avatar?d=mp";
		}
		String email = data.getEmail();
		return email == null ?
				"https://gravatar.com/avatar?d=mp" :
				"https://gravatar.com/avatar/" + md5(email) + "?d=identicon";
	}
}
