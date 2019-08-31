package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.http.*;
import io.global.forum.Utils.MustacheTemplater;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.pojo.AuthService;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.ThreadMetadata;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.NotNull;

import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.forum.Utils.redirect;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public final class PublicServlet {

	public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");

	public static AsyncServlet create(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					return forumDao.getThreads()
							.then(threads ->
									Promises.toList(threads.entrySet().stream()
											.map(e -> {
												ThreadDao dao = forumDao.getThreadDao(e.getKey());
												return dao != null ? getThreadInfo(e.getKey(), e.getValue(), dao) : null;
											})
											.filter(Objects::nonNull))
											.then(ts -> {
												List<ThreadInfo> sorted = ts.stream().sorted(Comparator.comparing(t -> t.getRoot().getInitialTimestamp())).collect(toList());
												return templater.render("threadList", map("threads", sorted));
											}));
				})
				.map(GET, "/thread/:threadID", postServlet(templater).then(parsePathIds()))
				.map(GET, "/thread/:threadID/:postID", postServlet(templater).then(parsePathIds()))
				.map(POST, "/thread/:threadID/:postID", parsePathIds().serve(request -> {
					Long tid = request.getAttachment("threadID");
					Long pid = request.getAttachment("postID");

					ForumDao forumDao = request.getAttachment(ForumDao.class);
					ThreadDao dao = forumDao.getThreadDao(tid);
					String content = request.getPostParameter("content");
					if (dao == null) {
						return Promise.of(HttpResponse.notFound404());
					}

					return dao.addPost(new UserId(AuthService.DK_APP_STORE, "ANON"), pid, content, emptyMap())
							.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/thread/" + tid + "/" + pid));
				}))
				.map(DELETE, "/thread/:threadID/:postID", parsePathIds().serve(request -> {
					Long tid = request.getAttachment("threadID");
					Long pid = request.getAttachment("postID");

					ForumDao forumDao = request.getAttachment(ForumDao.class);
					ThreadDao dao = forumDao.getThreadDao(tid);
					if (dao == null) {
						return Promise.of(HttpResponse.notFound404());
					}

					return dao.removePost(new UserId(AuthService.DK_APP_STORE, "ANON"), pid)
							.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/thread/" + tid + "/" + pid));
				}));
	}

	@NotNull
	private static AsyncServlet postServlet(MustacheTemplater templater) {
		return request -> {
			Long tid = request.getAttachment("threadID");
			Long pid = request.getAttachment("postID");
			if (pid != null && pid == 0L) {
				return Promise.of(redirect(request, request.getPathParameter("pubKey") + "/thread/" + tid));
			}
			ForumDao forumDao = request.getAttachment(ForumDao.class);
			ThreadDao dao = forumDao.getThreadDao(tid);
			if (dao == null) {
				return Promise.of(HttpResponse.notFound404());
			}

			return templater.render(pid != null ? "subthread" : "thread", map(
					"threadId", tid,
					"thread", dao.getThreadMetadata(),
					"post", dao.getPost(pid != null ? pid : 0L)
			));
		};
	}

	private static AsyncServletDecorator parsePathIds() {
		return servlet ->
				request -> {
					for (Map.Entry<String, String> entry : request.getPathParameters().entrySet()) {
						String key = entry.getKey();
						if (!key.toLowerCase().endsWith("id")) {
							continue;
						}
						try {
							Long id = Long.valueOf(entry.getValue());
							request.attach(key, id);
						} catch (NumberFormatException e) {
							return Promise.ofException(HttpException.ofCode(400, "Path parameter '" + key + "' (" + entry.getValue() + ") is not an ID"));
						}
					}
					return servlet.serve(request);
				};
	}

	private static Promise<ThreadInfo> getThreadInfo(Long tid, ThreadMetadata meta, ThreadDao dao) {
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
