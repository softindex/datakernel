package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.forum.Utils.MustacheSupplier;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.ThreadMetadata;

import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.forum.Utils.templated;
import static java.util.stream.Collectors.toList;

public final class PublicServlet {

	public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");

	public static AsyncServlet create(MustacheSupplier mustacheSupplier) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					Object context = request.getAttachment("mustacheContext");
					return forumDao.getThreads()
							.then(threads ->
									Promises.toList(threads.entrySet().stream()
											.map(e -> {
												ThreadDao dao = forumDao.getThreadDao(e.getKey());
												return dao != null ? getThreadInfo(e.getKey(), e.getValue(), dao) : null;
											})
											.filter(Objects::nonNull))
											.map(ts -> {
												List<ThreadInfo> sorted = ts.stream().sorted(Comparator.comparing(t -> t.getRoot().getInitialTimestamp())).collect(toList());
												return templated(mustacheSupplier, "threadList", map("context", context, "threads", sorted));
											}));
				})
				.map(GET, "/thread/:id", request -> {
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					Object context = request.getAttachment("mustacheContext");
					long tid = Long.parseLong(request.getPathParameter("id"));
					ThreadDao dao = forumDao.getThreadDao(tid);
					if (dao == null) {
						return Promise.of(HttpResponse.notFound404());
					}
					return dao.getThreadMetadata()
							.then(meta ->
									getThreadInfo(tid, meta, dao)
											.map(info -> templated(mustacheSupplier, "thread", map("context", context, "thread", info))));
				});
	}

	private static Promise<ThreadInfo> getThreadInfo(long tid, ThreadMetadata meta, ThreadDao dao) {
		return dao.getPost(0L).map(rootPost -> new ThreadInfo(tid, meta, rootPost));
	}

	public static class ThreadInfo {
		private final long id;
		private final ThreadMetadata meta;
		private final Post root;

		public ThreadInfo(long id, ThreadMetadata meta, Post root) {
			this.id = id;
			this.meta = meta;
			this.root = root;
		}

		public long getId() {
			return id;
		}

		public ThreadMetadata getMeta() {
			return meta;
		}

		public Post getRoot() {
			return root;
		}
	}
}
