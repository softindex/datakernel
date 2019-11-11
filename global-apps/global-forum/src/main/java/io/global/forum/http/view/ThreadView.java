package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserId;
import io.global.comm.pojo.UserRole;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

import static io.global.forum.util.Utils.formatInstant;

public class ThreadView {
	private final String id;
	private final String title;
	private final String lastUpdate;
	private final PostView root;

	public ThreadView(String id, String title, String lastUpdate, PostView root) {
		this.id = id;
		this.title = title;
		this.lastUpdate = lastUpdate;
		this.root = root;
	}

	public String getThreadId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public String getLastUpdate() {
		return lastUpdate;
	}

	public PostView getRoot() {
		return root;
	}

	private static Promise<@Nullable ThreadView> from(CommDao commDao, String threadId, ThreadMetadata threadMeta, @Nullable UserId currentUser, UserRole currentRole, int depth) {
		ThreadDao dao = commDao.getThreadDao(threadId);
		if (dao == null) {
			return null;
		}
		return dao.getPost("root")
				.then(rootPost -> {
					if (rootPost == null) {
						return Promise.of(null);
					}
					return PostView.from(commDao, rootPost, currentUser, currentRole, depth)
							.map(post -> new ThreadView(threadId, threadMeta.getTitle(), formatInstant(threadMeta.getLastUpdate()), post));
				});
	}

	public static Promise<@Nullable ThreadView> from(CommDao commDao, String threadId, @Nullable UserId currentUser, UserRole currentRole, int depth) {
		return commDao.getThreads("root").get(threadId)
				.then(threadMeta -> from(commDao, threadId, threadMeta, currentUser, currentRole, depth));
	}

	public static Promise<List<ThreadView>> from(CommDao commDao, int page, int limit, @Nullable UserId currentUser, UserRole currentRole, int depth) {
		return commDao.getThreads("root").slice(page * limit, limit)
				.then(threads -> Promises.toList(threads.stream()
						.map(e -> from(commDao, e.getKey(), e.getValue(), currentUser, currentRole, depth))
						.filter(Objects::nonNull)));
	}

	public static Promise<@Nullable ThreadView> root(CommDao commDao, String threadId, @Nullable UserId currentUser, UserRole currentRole) {
		return from(commDao, threadId, currentUser, currentRole, -1);
	}
}
