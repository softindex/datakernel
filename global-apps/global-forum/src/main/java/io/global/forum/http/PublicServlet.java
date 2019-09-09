package io.global.forum.http;

import io.datakernel.async.AsyncSupplier;
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

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.Utils.generateString;
import static io.global.forum.pojo.AuthService.DK_APP_STORE;
import static io.global.forum.util.Utils.redirectToReferer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

public final class PublicServlet {
	private static final String SESSION_ID = "FORUM_SID";
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";

	public static AsyncServlet create(AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					UserId userId = request.getAttachment(UserId.class);
					return forumDao.getThreads()
							.then(threads -> ThreadView.from(forumDao, threads, userId))
							.then(threads -> templater.render("threadList", map("threads", threads)));
				})
				.map(GET, "/login", request -> {
					if (request.getAttachment(UserId.class) != null) {
						return Promise.of(redirectToReferer(request, ".."));
					}
					return templater.render("login", map("loginScreen", true));
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
														String origin = request.getQueryParameter("origin");
														return redirect302(origin != null ? origin : "/" + pk)
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
								return redirectToReferer(request, "/" + pk)
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
								ThreadDao threadDao = forumDao.getThreadDao(tid);
								assert threadDao != null : "No thread dao just after creating the thread";

								Map<String, Attachment> attachmentMap = new HashMap<>();
								Map<String, String> paramsMap = new HashMap<>();

								return request.handleMultipart(AttachmentDataHandler.create(threadDao, paramsMap, attachmentMap))
										.then($ -> {
											String title = paramsMap.get("title");
											if (title == null || title.matches(WHITESPACE)) {
												return Promise.ofException(new ParseException(ThreadServlet.class, "'title' POST parameter is required"));
											}
											if (title.length() > 120) {
												return Promise.ofException(new ParseException(ThreadServlet.class, "Title is too long (" + title.length() + ">120)"));
											}

											String content = paramsMap.get("content");
											if ((content == null || content.matches(WHITESPACE)) && attachmentMap.isEmpty()) {
												return Promise.ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
											}
											String pk = forumDao.getKeys().getPubKey().asString();

											return forumDao.updateThread(tid, new ThreadMetadata(title))
													.then($2 -> threadDao.addRootPost(userId, content, attachmentMap))
													.map($2 -> redirect302("/" + pk + "/" + tid));
										})
										.thenEx(revertIfException(() -> threadDao.deleteAttachments(attachmentMap.keySet())))
										.thenEx(revertIfException(() -> forumDao.removeThread(tid)));
							});
				})
				.map(GET, "/profile", request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					return templater.render("profile");
				})
				.map(POST, "/profile", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					ForumDao forumDao = request.getAttachment(ForumDao.class);

					Map<String, String> params = request.getPostParameters();

					return forumDao.getUser(userId)
							.then(oldData -> {
								assert oldData != null; // TODO anton: check that

								String email = params.get("email");
								String username = params.get("username");
								String firstName = params.get("first_name");
								String lastName = params.get("last_name");

								if (username == null) {
									return Promise.ofException(HttpException.ofCode(401, "Username POST parameter is required"));
								}
								return forumDao.updateUser(userId, new UserData(oldData.getRole(), email, username, firstName, lastName));
							})
							.map($ -> redirectToReferer(request, "/" + forumDao.getKeys().getPubKey().asString() + "/profile"));
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
				.map("/:postID/*", postOperations(templater))
				.then(attachThreadDao());
	}

	private static HttpResponse postOpRedirect(HttpRequest request) {
		String tid = request.getPathParameter("threadID");
		String pid = request.getPathParameter("postID");
		ForumDao forumDao = request.getAttachment(ForumDao.class);
		return redirectToReferer(request, "/" + forumDao.getKeys().getPubKey().asString() + "/" + tid + "/" + pid);
	}

	@NotNull
	private static AsyncServlet postOperations(MustacheTemplater templater) {
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
								if ((content == null || content.matches(WHITESPACE)) && attachmentMap.isEmpty()) {
									return Promise.ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
								}
								return threadDao.addPost(user, pid, content, attachmentMap).toVoid();
							})
							.thenEx(revertIfException(() -> threadDao.deleteAttachments(attachmentMap.keySet())))
							.map($ -> postOpRedirect(request));
				})
				.map(GET, "/edit", request -> {
					String tid = request.getPathParameter("threadID");
					String pid = request.getPathParameter("postID");
					ForumDao forumDao = request.getAttachment(ForumDao.class);
					UserId userId = request.getAttachment(UserId.class);
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					return templater.render("subthread", map(
							"threadId", tid,
							"thread", threadDao.getThreadMetadata(),
							"post", threadDao.getPost(pid).then(post -> PostView.from(forumDao, post, userId, pid))
					));
				})
				.map(POST, "/delete", request ->
						request.getAttachment(ThreadDao.class).removePost(request.getAttachment(UserId.class), request.getPathParameter("postID"))
								.map($ -> postOpRedirect(request)))
				.map(POST, "/restore", request ->
						request.getAttachment(ThreadDao.class).restorePost(request.getPathParameter("postID"))
								.map($ -> postOpRedirect(request)))
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
			UserId userId = request.getAttachment(UserId.class);
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);
			return templater.render(pid != null ? "subthread" : "thread", map(
					"threadId", tid,
					"thread", threadDao.getThreadMetadata(),
					"post", threadDao.getPost(pid != null ? pid : "root").then(post -> PostView.from(forumDao, post, userId, null))
			));
		};
	}

	private static <T, E> BiFunction<T, Throwable, Promise<T>> revertIfException(AsyncSupplier<E> undo) {
		return (result, e) -> {
			if (e == null) {
				return Promise.of(result);
			}
			return undo.get()
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
					SessionStore<UserId> sessionStore = forumDao.getSessionStore();
					return sessionStore.get(sessionId)
							.then(userId -> {
								Duration maxAge;
								if (userId != null) {
									Promise<UserData> userDataPromise = forumDao.getUser(userId);
									templater.put("user", userDataPromise);
									templater.put("privileges", userDataPromise.map(userData -> {
										if (userData == null) {
											return null;
										}
										return userData.getRole().getPrivileges().stream()
												.collect(toMap(p -> p.name().toLowerCase(), $ -> true));
									}));

									request.attach(userId);
									maxAge = sessionStore.getSessionLifetime();
								} else {
									maxAge = Duration.ZERO;
								}
								return servlet.serve(request)
										.map(response -> {
											if (response.getCookie(SESSION_ID) != null) { // servlet itself had set the session (logout request)
												return response;
											}
											return response
													.withCookie(HttpCookie.of(SESSION_ID, sessionId)
															.withMaxAge(maxAge)
															.withPath("/" + forumDao.getKeys().getPubKey().asString()));
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
