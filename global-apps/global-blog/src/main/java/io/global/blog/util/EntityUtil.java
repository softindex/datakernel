package io.global.blog.util;

import io.datakernel.common.Preconditions;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.blog.http.view.BlogView;
import io.global.blog.http.view.PostView;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
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

import static io.datakernel.common.CollectorsEx.toMultimap;

public final class EntityUtil {
	private static final Comparator<Post> POST_COMPARATOR = Comparator.comparing(Post::getInitialTimestamp);

	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final MessageDigest MD5;

	static {
		try {
			MD5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ignored) {
			throw new AssertionError("Apparently, MD5 algorithm does not exist");
		}
	}

	public static Promise<List<BlogView>> blogViewListFrom(CommDao commDao, Map<String, ThreadMetadata> threads,
														   @Nullable UserId currentUser, Integer page, Integer size) {
		long offset = page.longValue() * size.longValue();
		offset = (int) offset != offset ? threads.size() : offset;
		return Promises.toList(threads
				.entrySet()
				.stream()
				.skip(offset)
				.limit(size)
				.map(e -> commDao.getThreadDao(e.getKey())
						.then(dao -> dao == null ?
								Promise.of(null) :
								dao.getPost("root").thenEx((rootPost, ex) -> {
									if (ex != null || rootPost == null) {
										return Promise.of(null);
									}
									int commentsCount = countChildrenDeep(rootPost);
									return postViewFrom(commDao, rootPost, currentUser, 0, null)
											.map(post -> new BlogView(e.getKey(), e.getValue(), post, commentsCount));
								}))
				))
				.map(list -> {
					Comparator<BlogView> comparator = Comparator.comparingLong(f -> f.getRoot().getInitialTimestampValue());
					return list.stream()
							.filter(Objects::nonNull)
							.sorted(comparator.reversed())
							.collect(Collectors.toList());
				});
	}

	private static int countChildrenDeep(Post rootPost) {
		int result = rootPost.getChildren().size();
		for (Post child : rootPost.getChildren()) {
			result += countChildrenDeep(child);
		}
		return result;
	}

	public static Promise<List<BlogView>> blogViewListFrom(CommDao commDao, Map<String, ThreadMetadata> threads, @Nullable UserId currentUser) {
		return EntityUtil.blogViewListFrom(commDao, threads, currentUser, 0, threads.size());
	}


	public static Promise<PostView> postViewFrom(CommDao commDao, Post post, @Nullable UserId currentUser, int childrenDepth, @Nullable PostView parent) {
		boolean deepest = childrenDepth == 0;
		UserId deleterId = post.getDeletedBy();
		Promise<@Nullable UserData> authorPromise = commDao.getUsers().get(post.getAuthor());
		Promise<@Nullable UserData> deleterPromise = deleterId != null ? commDao.getUsers().get(deleterId) : Promise.of(null);
		Promise<@Nullable UserData> currentPromise = currentUser != null ? commDao.getUsers().get(currentUser) : Promise.of(null);
		return Promises.toTuple(authorPromise, deleterPromise, currentPromise)
				.then(users -> {
					UserData author = users.getValue1();
					UserData deleter = users.getValue2();
					UserData current = users.getValue3();
					PostView root = postViewFrom(author, deleter, current, post, currentUser, deleterId, parent, false);
					Promise<List<PostView>> childrenPromise = deepest ?
							collectAllDeepestChildren(root, post, commDao, currentUser, current) :
							Promises.toList(post
									.getChildren()
									.stream()
									.sorted(POST_COMPARATOR.reversed())
									.map(p -> postViewFrom(commDao, p, currentUser, childrenDepth - 1, root)));
					return childrenPromise.map(root::withChildren);
				});
	}

	private static Promise<List<PostView>> collectAllDeepestChildren(PostView root, Post post, CommDao commDao, @Nullable UserId currentUser, UserData current) {
		ArrayList<PostView> result = new ArrayList<>();
		return doCollectChildrenPostView(root, post, commDao, currentUser, current, result)
				.map($ -> result);
	}

	private static Promise<Void> doCollectChildrenPostView(PostView root, Post post, CommDao commDao, @Nullable UserId currentUser,
														   UserData current, List<PostView> resultList) {
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
								PostView postView = postViewFrom(author, deleter, current, children, currentUser, deleterId, root, true)
										.withChildren(Collections.emptyList());
								resultList.add(postView);
								return doCollectChildrenPostView(postView, children, commDao, currentUser, current, resultList);
							});
				}));
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

	private static PostView postViewFrom(UserData author, UserData deleter, UserData current, Post post,
										 @Nullable UserId currentUser, @Nullable UserId deleterUser, @Nullable PostView parent, boolean deepest) {
		UserRole role = current != null ? current.getRole() : UserRole.GUEST;
		boolean own = Objects.equals(post.getAuthor(), currentUser);
		boolean editable = role.isPrivileged() || own && role.isKnown() && (deleterUser == null || post.getAuthor().equals(deleterUser));
		boolean deletedVisible = role.isPrivileged() || own && role.isKnown();
		return new PostView(post.getId(), post.getContent(),
				deepest, format(post.getInitialTimestamp()),
				format(post.getLastEditTimestamp()),
				author != null ? author.getUsername() : null,
				post.getAuthor().getAuthId(),
				avatarUrl(author),
				convert(post.getAttachments()),
				deleter != null ? deleter.getUsername() : null,
				parent,
				editable,
				deletedVisible,
				post.getInitialTimestamp()
		);
	}

	private static Map<String, Set<String>> convert(Map<String, AttachmentType> attachments) {
		return attachments.entrySet().stream()
				.sorted(Comparator.comparing(Map.Entry::getKey))
				.collect(toMultimap(entry -> entry.getValue().toString().toLowerCase(), Map.Entry::getKey));
	}
}
