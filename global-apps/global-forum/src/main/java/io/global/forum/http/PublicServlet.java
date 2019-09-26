package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.util.ref.Ref;
import io.global.appstore.AppStore;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.http.AttachmentDataHandler;
import io.global.comm.pojo.*;
import io.global.common.PubKey;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.http.view.IpBanView;
import io.global.forum.http.view.PostView;
import io.global.forum.http.view.ThreadView;
import io.global.forum.http.view.UserView;
import io.global.forum.ot.ForumMetadata;
import io.global.mustache.MustacheTemplater;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.*;

import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpHeaders.HOST;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.Utils.nullToEmpty;
import static io.global.Utils.*;
import static io.global.comm.dao.ThreadDao.ATTACHMENT_NOT_FOUND;
import static io.global.comm.pojo.AuthService.DK_APP_STORE;
import static io.global.forum.util.Utils.*;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

public final class PublicServlet {
	private static final String SESSION_ID = "FORUM_SID";
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";

	public static AsyncServlet create(String appStoreUrl, AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					CommDao commDao = request.getAttachment(CommDao.class);
					UserId userId = request.getAttachment(UserId.class);
					return commDao.getThreads()
							.then(threads -> ThreadView.from(commDao, threads, userId, request.getAttachment(UserRole.class)))
							.then(threads -> templater.render("thread_list", map("threads", threads)));
				})

				.map("/admin/*", adminServlet(templater))

				.map(GET, "/login", request -> {

					String origin = request.getQueryParameter("origin");
					if (origin == null) {
						origin = request.getHeader(REFERER);
					}
					if (origin == null) {
						origin = "/";
					}

					if (request.getAttachment(UserId.class) != null) {
						return Promise.of(redirect302(origin));
					}
					return templater.render("login", map("loginScreen", true, "origin", origin));
				})
				.map(GET, "/authorize", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStore.exchangeAuthToken(token)
							.then(profile -> {
								CommDao commDao = request.getAttachment(CommDao.class);
								PubKey containerPubKey = commDao.getKeys().getPubKey();
								PubKey pubKey = profile.getPubKey();
								UserId userId = new UserId(DK_APP_STORE, pubKey.asString());

								String sessionId = generateString(32);

								return commDao
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
											return commDao.updateUser(userId, userData);
										})
										.then($ -> {
											SessionStore<UserId> sessionStore = commDao.getSessionStore();
											return sessionStore.save(sessionId, userId)
													.map($2 -> {
														String origin = request.getQueryParameter("origin");
														return redirect302(origin != null ? origin : "/")
																.withCookie(HttpCookie.of(SESSION_ID, sessionId)
																		.withPath("/")
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
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> redirectToReferer(request, "/")
									.withCookie(HttpCookie.of(SESSION_ID)
											.withPath("/")
											.withMaxAge(Duration.ZERO)));
				})
				.map(GET, "/new", request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("new_thread", map("creatingNewThread", true));
				})
				.map(POST, "/new", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					CommDao commDao = request.getAttachment(CommDao.class);

					return commDao.generateThreadId()
							.then(id -> commDao.updateThread(id, new ThreadMetadata("<unnamed>"))
									.map($ -> id))
							.then(tid -> {
								ThreadDao threadDao = commDao.getThreadDao(tid);
								assert threadDao != null : "No thread dao just after creating the thread";

								Map<String, AttachmentType> attachmentMap = new HashMap<>();
								Map<String, String> paramsMap = new HashMap<>();

								return request.handleMultipart(AttachmentDataHandler.create(threadDao, "root", paramsMap, attachmentMap, true))
										.then($ -> {
											String title = paramsMap.get("title");
											if (title == null || title.matches(WHITESPACE)) {
												return Promise.ofException(new ParseException(PublicServlet.class, "'title' POST parameter is required"));
											}
											if (title.length() > 120) {
												return Promise.ofException(new ParseException(PublicServlet.class, "Title is too long (" + title.length() + ">120)"));
											}

											String content = paramsMap.get("content");
											if ((content == null || content.matches(WHITESPACE)) && attachmentMap.isEmpty()) {
												return Promise.ofException(new ParseException(PublicServlet.class, "'content' POST parameter is required"));
											}

											return commDao.updateThread(tid, new ThreadMetadata(title))
													.then($2 -> threadDao.addRootPost(userId, content, attachmentMap))
													.then($2 -> threadDao.updateRating(userId, "root", Rating.LIKE))
													.map($2 -> redirect302("/" + tid));
										})
										.thenEx(revertIfException(() -> threadDao.deleteAttachments("root", attachmentMap.keySet())))
										.thenEx(revertIfException(() -> commDao.removeThread(tid)));
							});
				})
				.map(GET, "/profile", request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("profile", map("shownUser", new Ref<>("user")));
				})
				.map(GET, "/profile/:userId", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.of(redirectToLogin(request));
					}
					if (!request.getAttachment(UserRole.class).isPrivileged()) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					UserId shownUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					return commDao
							.getUser(shownUserId)
							.then(shownUser -> shownUser != null ?
									templater.render("profile", map("shownUser", UserView.from(shownUserId, shownUser))) :
									Promise.ofException(HttpException.ofCode(400, "No such user")));
				})
				.map(POST, "/profile/:userId", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					UserId updatingUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					if (!(userId.equals(updatingUserId) || request.getAttachment(UserRole.class).isPrivileged())) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}

					Map<String, String> params = request.getPostParameters();
					String email = params.get("email");
					String username = params.get("username");
					String firstName = params.get("first_name");
					String lastName = params.get("last_name");
					if (email == null || email.matches(WHITESPACE)) {
						return Promise.ofException(HttpException.ofCode(401, "Email POST parameter is required"));
					}
					if (username == null || username.matches(WHITESPACE)) {
						return Promise.ofException(HttpException.ofCode(401, "Username POST parameter is required"));
					}
					CommDao commDao = request.getAttachment(CommDao.class);

					return commDao.getUser(updatingUserId)
							.then(oldData -> {
								if (oldData == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								return commDao.updateUser(updatingUserId, new UserData(oldData.getRole(), email, username, firstName, lastName))
										.map($ -> redirectToReferer(request, "/"));
							});
				})
				.map("/:threadID/*", RoutingServlet.create()
						.map(GET, "/", postViewServlet(templater))
						.map(GET, "/:postID/", postViewServlet(templater))
						.map("/:postID/*", postOperations())
						.then(attachThreadDao()))
				.then(session(templater))
				.then(setup(appStoreUrl, templater));
	}

	private static HttpResponse redirectToLogin(HttpRequest request) {
		return redirect302("/login?origin=" + request.getPath());
	}

	private static HttpResponse postOpRedirect(HttpRequest request) {
		String tid = request.getPathParameter("threadID");
		return redirectToReferer(request, "/" + tid);
	}

	private static AsyncServlet postOperations() {
		return RoutingServlet.create()
				.map(POST, "/", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					Map<String, AttachmentType> attachmentMap = new HashMap<>();
					Map<String, String> paramsMap = new HashMap<>();

					UserId user = request.getAttachment(UserId.class);
					String parentId = request.getPathParameter("postID");

					return threadDao.generatePostId()
							.then(postId -> request.handleMultipart(AttachmentDataHandler.create(threadDao, postId, paramsMap, attachmentMap, true))
									.then($ -> {
										String content = paramsMap.get("content");
										if ((content == null || content.matches(WHITESPACE)) && attachmentMap.isEmpty()) {
											return Promise.ofException(new ParseException(PublicServlet.class, "'content' POST parameter is required"));
										}
										return threadDao.addPost(user, parentId, postId, content, attachmentMap)
												.then($2 -> threadDao.updateRating(user, postId, Rating.LIKE));
									}))
							.thenEx(revertIfException(() -> threadDao.deleteAttachments(parentId, attachmentMap.keySet())))
							.map($ -> postOpRedirect(request));
				})
				.map(POST, "/rate/:rating", request -> {
					String pid = request.getPathParameter("postID");
					return request.getAttachment(ThreadDao.class)
							.updateRating(request.getAttachment(UserId.class), pid, Rating.fromString(request.getPathParameter("rating")))
							.map($ -> postOpRedirect(request));
				})
				.map(POST, "/delete", request -> {
					String pid = request.getPathParameter("postID");
					return request.getAttachment(ThreadDao.class)
							.removePost(request.getAttachment(UserId.class), pid)
							.map($ -> postOpRedirect(request));
				})
				.map(POST, "/restore", request -> {
					String pid = request.getPathParameter("postID");
					return request.getAttachment(ThreadDao.class)
							.restorePost(pid)
							.map($ -> postOpRedirect(request));
				})
				.map(POST, "/edit", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					String pid = request.getPathParameter("postID");

					Map<String, AttachmentType> attachmentMap = new HashMap<>();
					Map<String, String> paramsMap = new HashMap<>();

					return threadDao.listAttachments(pid)
							.then(existing -> request.handleMultipart(AttachmentDataHandler.create(threadDao, pid, existing, paramsMap, attachmentMap, true)))
							.then($ -> {
								String content = paramsMap.get("content");
								if ((content == null || content.matches(WHITESPACE)) && attachmentMap.isEmpty()) {
									return Promise.ofException(new ParseException(PublicServlet.class, "'content' POST parameter is required"));
								}
								Set<String> removed = Arrays.stream(nullToEmpty(paramsMap.get("removeAttachments")).split(","))
										.map(String::trim)
										.collect(toSet());

								return threadDao.updatePost(pid, content, attachmentMap, removed)
										.map($2 -> postOpRedirect(request));
							})
							.thenEx(revertIfException(() -> threadDao.deleteAttachments(pid, attachmentMap.keySet())));
				})
				.map(GET, "/download/:filename", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					String postId = request.getPathParameter("postID");
					String filename = request.getPathParameter("filename");

					return threadDao.attachmentSize(postId, filename)
							.thenEx((size, e) -> {
								if (e == null) {
									return HttpResponse.file(
											(offset, limit) -> threadDao.loadAttachment(postId, filename, offset, limit),
											filename,
											size,
											request.getHeader(HttpHeaders.RANGE)
									);
								} else if (e == ATTACHMENT_NOT_FOUND) {
									return Promise.of(HttpResponse.notFound404());
								} else {
									return Promise.<HttpResponse>ofException(e);
								}
							});
				})
				.then(servlet -> request -> {
					if (request.getMethod() == POST && request.getAttachment(UserId.class) == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					return servlet.serve(request);
				});
	}

	private static AsyncServlet adminServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> templater.render("admin_panel"))

				.map(GET, "/ip-bans", request -> templater.render("ip_bans", map("bans", IpBanView.from(request.getAttachment(CommDao.class)))))

				.map(GET, "/edit-forum", request -> templater.render("edit_forum", emptyMap()))

				.map(POST, "/edit-forum", request -> {
					try {
						String title = getRequiredPostParameter(request, "title");
						String description = getRequiredPostParameter(request, "description");
						return request.getAttachment(ForumDao.class)
								.setForumMetadata(new ForumMetadata(title, description))
								.map($ -> redirect302("."));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/ip-bans/new", request -> templater.render("ip_ban", map("ban.id", "new", "ban.ip", "a new range")))

				.map(GET, "/ip-bans/:id", request ->
						IpBanView.from(request.getAttachment(CommDao.class), request.getPathParameter("id"))
								.then(view -> view != null ?
										templater.render("ip_ban", map("ban", view)) :
										Promise.of(redirect302("."))))

				.map(POST, "/ip-bans/:id", request -> {
					String action;
					try {
						action = getRequiredPostParameter(request, "action");
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
					switch (action) {
						case "unban": {
							String id = request.getPathParameter("id");
							return request.getAttachment(CommDao.class)
									.unbanIpRange(id)
									.map($ -> redirect302("."));
						}
						case "save": {
							try {
								byte[] ip = parse4Bytes(request, "ip");
								byte[] mask = parse4Bytes(request, "mask");
								String reason = getRequiredPostParameter(request, "reason");
								Instant until = LocalDateTime.parse(getRequiredPostParameter(request, "until"), DATE_TIME_FORMAT).atZone(ZoneId.systemDefault()).toInstant();

								CommDao commDao = request.getAttachment(CommDao.class);

								String id = request.getPathParameter("id");

								Promise<Void> unban = "new".equals(id) ? Promise.complete() : commDao.unbanIpRange(id);

								return unban
										.then($ -> commDao.banIpRange(new IpRange(ip, mask), request.getAttachment(UserId.class), until, reason))
										.map($ -> redirect302("."));

							} catch (ParseException e) {
								return Promise.ofException(e);
							} catch (DateTimeParseException e) {
								return Promise.ofException(new ParseException("Invalid datetime: " + e.getMessage()));
							}
						}
						default: {
							return Promise.ofException(HttpException.ofCode(400, "Unknown action '" + action + "'"));
						}
					}
				})
				.then(servlet ->
						request -> {
							if (request.getAttachment(UserId.class) == null) {
								return Promise.of(redirectToLogin(request));
							}
							if (!request.getAttachment(UserRole.class).isPrivileged()) {
								return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
							}
							return servlet.serve(request);
						});
	}

	private static AsyncServlet postViewServlet(MustacheTemplater templater) {
		return request -> {
			String tid = request.getPathParameter("threadID");
			String pid = request.getPathParameters().get("postID");
			CommDao commDao = request.getAttachment(CommDao.class);
			if ("root".equals(pid)) {
				return Promise.of(redirect302("."));
			}
			UserId userId = request.getAttachment(UserId.class);
			UserRole role = request.getAttachment(UserRole.class);
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);
			return templater.render(pid != null ? "subthread" : "thread", map(
					"threadId", tid,
					"thread", threadDao.getThreadMetadata(),
					"post", threadDao.getPost(pid != null ? pid : "root")
							.then(post -> PostView.from(commDao, post, userId, role, 100))));
		};
	}

	private static AsyncServletDecorator setup(String appStoreUrl, MustacheTemplater templater) {
		return onRequest(request -> {
			ForumDao forumDao = request.getAttachment(ForumUserContainer.class).getForumDao();
			request.attach(ForumDao.class, forumDao);
			request.attach(CommDao.class, forumDao.getCommDao());
			PubKey pubKey = forumDao.getKeys().getPubKey();
			request.attach(PubKey.class, pubKey);
			templater.clear();
			templater.put("appStoreUrl", appStoreUrl);

			String host = request.getHeader(HOST);
			if (host == null) {
				host = request.getHostAndPort();
			}
			assert host != null : "host should not be null here";

			templater.put("url", host + request.getPathAndQuery());
			templater.put("url.host", host);
			templater.put("forum", forumDao.getForumMetadata());
		});
	}

	private static AsyncServletDecorator session(MustacheTemplater templater) {
		return servlet ->
				request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						request.attach(UserRole.GUEST);
						return servlet.serve(request);
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					SessionStore<UserId> sessionStore = commDao.getSessionStore();
					return sessionStore.get(sessionId)
							.then(userId -> {
								Promise<Duration> maxAge;
								if (userId != null) {
									maxAge = Promises.toTuple(commDao.updateUserLastIp(userId, request.getRemoteAddress()), commDao.getUser(userId))
											.map(t -> {
												UserData user = t.getValue2();
												templater.put("user", UserView.from(userId, user));
												request.attach(userId);
												request.attach(user);
												request.attach(user.getRole());
												return sessionStore.getSessionLifetime();
											});
								} else {
									request.attach(UserRole.GUEST);
									maxAge = Promise.of(Duration.ZERO);
								}
								return maxAge.then(m ->
										servlet.serve(request)
												.map(response -> {
													if (response.getCookie(SESSION_ID) != null) { // servlet itself had set the session (logout request)
														return response;
													}
													return response
															.withCookie(HttpCookie.of(SESSION_ID, sessionId)
																	.withMaxAge(m)
																	.withPath("/"));
												}));
							});
				};
	}

	private static AsyncServletDecorator attachThreadDao() {
		return servlet ->
				request -> {
					String id = request.getPathParameter("threadID");
					ThreadDao dao = request.getAttachment(CommDao.class).getThreadDao(id);
					if (dao == null) {
						return Promise.ofException(HttpException.ofCode(404, "No thread with id " + id));
					}
					request.attach(ThreadDao.class, dao);
					return servlet.serve(request);
				};
	}
}
