package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.forum.Utils.MustacheTemplater;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.http.view.PostView;
import io.global.forum.http.view.ThreadView;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.AuthService;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.forum.Utils.redirect;

public final class PublicServlet {
	public static AsyncServlet create(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					return forumDao.getThreads()
							.then(threads -> ThreadView.from(forumDao, threads))
							.then(threads -> templater.render("threadList", map("threads", threads)));
				})
				.map("/:threadID/*", RoutingServlet.create()
						.map(GET, "/", postServlet(templater).then(parsePathIds()))
						.map("/:postID/*", RoutingServlet.create()
								.map(GET, "/", postServlet(templater))
								.map(GET, "/download/:identifier", request -> {
									ThreadDao threadDao = request.getAttachment(ThreadDao.class);
									Long pid = request.getAttachment("postID");
									String gfsid = request.getPathParameter("identifier");

									return threadDao.getAttachment(pid, gfsid)
											.then(attachment -> threadDao.attachmentSize(gfsid)
													.then(size -> {
														try {
															return Promise.of(HttpResponse.file(
																	(offset, limit) ->
																			threadDao.loadAttachment(gfsid, offset, limit),
																	attachment.getFilename(),
																	size,
																	request.getHeader(HttpHeaders.RANGE)
															));
														} catch (HttpException e) {
															return Promise.<HttpResponse>ofException(e);
														}
													}));
								})
								.map(POST, "/", request -> {
									ThreadDao dao = request.getAttachment(ThreadDao.class);

									Map<String, Attachment> attachmentMap = new HashMap<>();
									Map<String, String> paramsMap = new HashMap<>();

									UserId user = new UserId(AuthService.DK_APP_STORE, "ANON");
									Long tid = request.getAttachment("threadID");
									Long pid = request.getAttachment("postID");

									return request.handleMultipart(AttachmentDataHandler.create(dao, paramsMap, attachmentMap))
											.then($ -> {
												String content = paramsMap.get("content");
												if (content == null) {
													return Promise.ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
												}
												return dao.addPost(user, pid, content, attachmentMap).toVoid();
											})
											.thenEx(revertIfException(dao, attachmentMap))
											.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/" + tid + "/" + pid));
								})
								.map(DELETE, "/", parsePathIds().serve(request -> {
									Long tid = request.getAttachment("threadID");
									Long pid = request.getAttachment("postID");

									ThreadDao dao = request.getAttachment(ThreadDao.class);

									UserId user = new UserId(AuthService.DK_APP_STORE, "ANON");
									return dao.removePost(user, pid)
											.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/" + tid + "/" + pid));
								}))
								.map(POST, "/restore", parsePathIds().serve(request -> {
									Long tid = request.getAttachment("threadID");
									Long pid = request.getAttachment("postID");

									ThreadDao dao = request.getAttachment(ThreadDao.class);

									return dao.restorePost(pid)
											.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/" + tid + "/" + pid));
								}))
								.then(parsePathIds()))
						.then(attachThreadDao()));
	}

	@NotNull
	private static AsyncServlet postServlet(MustacheTemplater templater) {
		return request -> {
			Long tid = request.getAttachment("threadID");
			Long pid = request.getAttachment("postID");
			if (pid != null && pid == 0L) {
				return Promise.of(redirect(request, request.getPathParameter("pubKey") + "/" + tid));
			}
			ForumDao forumDao = request.getAttachment(ForumDao.class);
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);

			return templater.render(pid != null ? "subthread" : "thread", map(
					"threadId", tid,
					"thread", threadDao.getThreadMetadata(),
					"post", threadDao.getPost(pid != null ? pid : 0L).then(post -> PostView.from(forumDao, post))
			));
		};
	}

	private static BiFunction<Void, Throwable, Promise<Void>> revertIfException(ThreadDao threadDao, Map<String, Attachment> attachments) {
		return ($, e) -> {
			if (e == null) {
				return Promise.complete();
			}
			return threadDao.deleteAttachments(attachments.keySet())
					.thenEx(($2, e2) -> {
						if (e2 != null) {
							e.addSuppressed(e2);
						}
						return Promise.ofException(e);
					});
		};
	}

	private static AsyncServletDecorator attachThreadDao() {
		return servlet ->
				request -> {
					String sid = request.getPathParameter("threadID");
					try {
						long id = Long.parseLong(sid);
						ForumDao forumDao = request.getAttachment(ForumDao.class);
						ThreadDao dao = forumDao.getThreadDao(id);
						if (dao == null) {
							return Promise.ofException(HttpException.ofCode(404, "No thread with id " + id));
						}
						request.attach(ThreadDao.class, dao);
						return servlet.serve(request);
					} catch (NumberFormatException e) {
						return Promise.ofException(HttpException.ofCode(400, "Path parameter 'threadID' (" + sid + ") is not an ID"));
					}
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
}
