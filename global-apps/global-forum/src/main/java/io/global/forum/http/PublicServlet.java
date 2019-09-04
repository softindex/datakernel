package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.global.appstore.AppStore;
import io.global.common.PubKey;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.http.view.PostView;
import io.global.forum.http.view.ThreadView;
import io.global.forum.pojo.*;
import io.global.forum.util.MustacheTemplater;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.Utils.generateString;
import static io.global.forum.pojo.AuthService.DK_APP_STORE;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PublicServlet {
	private static final String SESSION_ID = "SESSION_ID";

	public static AsyncServlet create(AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					return forumDao.getThreads()
							.then(threads -> ThreadView.from(forumDao, threads))
							.then(threads -> templater.render("threadList", map("threads", threads)));
				})
				.map(GET, "/authorize", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStore.exchangeAuthToken(token)
							.then(profile -> {
								ForumDao forumDao = request.getAttachment(ForumDao.class);
								PubKey containerPubKey = forumDao.getKeys().getPubKey();
								PubKey pubKey = profile.getPubKey();
								UserId userId = new UserId(DK_APP_STORE, pubKey.asString());

								String sessionId = generateString(32);

								return forumDao
										.getUser(userId)
										.then(existing -> {
											if (existing != null) {
												return Promise.complete();
											}
											UserRole userRole = containerPubKey.equals(pubKey) ?
													UserRole.OWNER :
													UserRole.COMMON;

											UserData userData = new UserData(userRole, profile.getEmail(),
													profile.getUsername(), profile.getFirstName(), profile.getLastName(), null);
											return forumDao.updateUser(userId, userData);
										})
										.then($ -> {
											SessionStore<UserId> sessionStore = forumDao.getSessionStore();
											return sessionStore.save(sessionId, userId)
													.map($2 -> {
														String pk = forumDao.getKeys().getPubKey().asString();
														return HttpResponse.redirect302("/" + pk)
																.withCookie(HttpCookie.of(SESSION_ID, sessionId)
																		.withPath("/" + pk)
																		.withMaxAge(sessionStore.getSessionLifetime()));
													});
										});
							});
				})
				.map(POST, "/logout", request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(HttpResponse.ok200());
					}
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					return forumDao.getSessionStore().remove(sessionId)
							.map($ -> {
								String pk = forumDao.getKeys().getPubKey().asString();
								return HttpResponse.redirect302("/" + pk)
										.withCookie(HttpCookie.of(SESSION_ID, "<unset>")
												.withPath("/" + pk)
												.withMaxAge(Duration.ZERO));
							});
				})

				.map(GET, "/new", request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					return templater.render("newThread", map("creatingNewThread", true));
				})
				.map(POST, "/new", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
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
													.then($2 -> dao.addRootPost(userId, content, attachmentMap))
													.thenEx(revertIfException(dao, attachmentMap))
													.map($2 -> redirect302("../" + tid));
										});
							});
				})
				.map("/:threadID/*", threadServlet(templater))
				.then(session(templater))
				.then(servlet ->
						request -> {
							ForumDao forumDao = request.getAttachment(ForumDao.class);
							templater.put("pubKey", forumDao.getKeys().getPubKey().asString());
							templater.put("forum", forumDao.getForumMetadata());

							return servlet.serve(request)
									.thenEx((response, e) -> {
										if (e instanceof HttpException && ((HttpException) e).getCode() == 404) {
											return templater.render(404, "404", map("message", e.getMessage()));
										}
										if (e != null) {
											return Promise.<HttpResponse>ofException(e);
										}
										if (response.getCode() != 404) {
											return Promise.of(response);
										}
										String message = response.isBodyLoaded() ? response.getBody().asString(UTF_8) : "";
										return templater.render(404, "404", map("message", message.isEmpty() ? null : message));
									});
						});
	}

	private static AsyncServlet threadServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", postViewServlet(templater))
				.map(GET, "/:postID/", postViewServlet(templater))
				.map("/:postID/*", postOperations())
				.then(attachThreadDao());
	}

	@NotNull
	private static AsyncServlet postOperations() {
		return RoutingServlet.create()
				.map(POST, "/", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					Map<String, Attachment> attachmentMap = new HashMap<>();
					Map<String, String> paramsMap = new HashMap<>();

					UserId user = request.getAttachment(UserId.class);
					String pid = request.getPathParameter("postID");

					return request.handleMultipart(AttachmentDataHandler.create(threadDao, paramsMap, attachmentMap))
							.then($ -> {
								String content = paramsMap.get("content");
								if (content == null) {
									return Promise.ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
								}
								return threadDao.addPost(user, pid, content, attachmentMap).toVoid();
							})
							.thenEx(revertIfException(threadDao, attachmentMap))
							.map($ -> {
								String referer = request.getHeader(REFERER);
								return redirect302(referer != null ? referer : ".");
							});
				})
				.map(POST, "/delete", request -> {
					String pid = request.getPathParameter("postID");

					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					UserId user = request.getAttachment(UserId.class);
					return threadDao.removePost(user, pid)
							.map($ -> redirect302("../.."));
				})
				.map(POST, "/restore", request -> {
					String pid = request.getPathParameter("postID");

					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					return threadDao.restorePost(pid)
							.map($ -> redirect302("../.."));
				})
				.map(GET, "/download/:tag", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					String pid = request.getPathParameter("postID");
					String tag = request.getPathParameter("tag");

					return threadDao.getAttachment(pid, tag)
							.then(attachment -> threadDao.attachmentSize(tag)
									.then(size ->
											HttpResponse.file(
													(offset, limit) -> threadDao.loadAttachment(tag, offset, limit),
													attachment.getFilename(),
													size,
													request.getHeader(HttpHeaders.RANGE)
											)));
				})
				.then(servlet -> request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					return servlet.serve(request);
				});
	}

	@NotNull
	private static AsyncServlet postViewServlet(MustacheTemplater templater) {
		return request -> {
			String tid = request.getPathParameter("threadID");
			String pid = request.getPathParameters().get("postID");
			ForumDao forumDao = request.getAttachment(ForumDao.class);
			if ("root".equals(pid)) {
				return Promise.of(redirect302("../"));
			}

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

	private static AsyncServletDecorator session(MustacheTemplater templater) {
		return servlet ->
				request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return servlet.serve(request);
					}
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					String pubKey = forumDao.getKeys().getPubKey().asString();
					SessionStore<UserId> sessionStore = forumDao.getSessionStore();
					return sessionStore.get(sessionId)
							.then(userId -> {
								if (userId != null) {
									templater.put("user", forumDao.getUser(userId));
									request.attach(userId);
								} else {
									templater.remove("user");
								}
								return servlet.serve(request)
										.map(response -> {
											Duration maxAge = userId != null ?
													sessionStore.getSessionLifetime() :
													Duration.ZERO;
											return response
													.withCookie(HttpCookie.of(SESSION_ID, sessionId)
															.withMaxAge(maxAge)
															.withPath("/" + pubKey));
										});
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
