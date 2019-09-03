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
import io.global.forum.pojo.ThreadMetadata;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
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
				.map(GET, "/new", request -> templater.render("newThread", map("creatingNewThread", true)))
				.map(POST, "/new", request -> {

					UserId user = new UserId(AuthService.DK_APP_STORE, "ANON");
					ForumDao forumDao = request.getAttachment(ForumDao.class);

					return forumDao.createThread(new ThreadMetadata("<unnamed>"))
							.then(tid -> {
								ThreadDao dao = forumDao.getThreadDao(tid);
								assert dao != null : "No thread dao just after creating the thread";

								Map<String, Attachment> attachmentMap = new HashMap<>();
								Map<String, String> paramsMap = new HashMap<>();

								return request.handleMultipart(AttachmentDataHandler.create(dao, paramsMap, attachmentMap))
										.then($ -> {
											String title = paramsMap.get("title");
											if (title == null) {
												return Promise.<HttpResponse>ofException(new ParseException(ThreadServlet.class, "'title' POST parameter is required"));
											}

											String content = paramsMap.get("content");
											if (content == null) {
												return Promise.<HttpResponse>ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
											}

											return forumDao.updateThread(tid, new ThreadMetadata(title))
													.then($2 -> dao.addRootPost(user, content, attachmentMap))
													.thenEx(revertIfException(dao, attachmentMap))
													.map($2 -> redirect302("/" + request.getPathParameter("pubKey") + "/" + tid));
										});
							});
				})
				.map("/:threadID/*", RoutingServlet.create()
						.map(GET, "/", postServlet(templater))
						.map("/:postID/*", RoutingServlet.create()
								.map(GET, "/", postServlet(templater))
								.map(GET, "/download/:tag", request -> {
									ThreadDao threadDao = request.getAttachment(ThreadDao.class);
									String pid = request.getPathParameter("postID");
									String tag = request.getPathParameter("tag");

									return threadDao.getAttachment(pid, tag)
											.then(attachment -> threadDao.attachmentSize(tag)
													.then(size -> {
														try {
															return Promise.of(HttpResponse.file(
																	(offset, limit) -> threadDao.loadAttachment(tag, offset, limit),
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
									String tid = request.getPathParameter("threadID");
									String pid = request.getPathParameter("postID");

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
								.map(POST, "/delete", request -> {
									String tid = request.getPathParameter("threadID");
									String pid = request.getPathParameter("postID");

									ThreadDao dao = request.getAttachment(ThreadDao.class);

									UserId user = new UserId(AuthService.DK_APP_STORE, "ANON");
									return dao.removePost(user, pid)
											.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/" + tid + "/" + pid));
								})
								.map(POST, "/restore", request -> {
									String tid = request.getPathParameter("threadID");
									String pid = request.getPathParameter("postID");

									ThreadDao dao = request.getAttachment(ThreadDao.class);

									return dao.restorePost(pid)
											.map($ -> redirect(request, "/" + request.getPathParameter("pubKey") + "/" + tid + "/" + pid));
								}))
						.then(attachThreadDao()));
	}

	@NotNull
	private static AsyncServlet postServlet(MustacheTemplater templater) {
		return request -> {
			String tid = request.getPathParameter("threadID");
			String pid = request.getPathParameters().get("postID");
			if ("root".equals(pid)) {
				return Promise.of(redirect(request, request.getPathParameter("pubKey") + "/" + tid));
			}
			ForumDao forumDao = request.getAttachment(ForumDao.class);
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);

			return templater.render(pid != null ? "subthread" : "thread", map(
					"threadId", tid,
					"thread", threadDao.getThreadMetadata(),
					"post", threadDao.getPost(pid != null ? pid : "root").then(post -> PostView.from(forumDao, post))
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
					String id = request.getPathParameter("threadID");
					ThreadDao dao = request.getAttachment(ForumDao.class).getThreadDao(id);
					if (dao == null) {
						return Promise.ofException(HttpException.ofCode(404, "No thread with id " + id));
					}
					request.attach(ThreadDao.class, dao);
					return servlet.serve(request);
				};
	}
}
