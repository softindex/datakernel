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
import io.global.forum.util.Utils;
import io.global.mustache.MustacheTemplater;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
						return Promise.ofException(HttpException.badRequest400("No token"));
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
					return templater.render("new_thread", emptyMap());
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
											try {
												String title = getPostParameter(paramsMap, "title", 120);
												String content = getPostParameter(paramsMap, "content", 4000);

												return commDao.updateThread(tid, new ThreadMetadata(title))
														.then($2 -> threadDao.addRootPost(userId, content, attachmentMap))
														.then($2 -> threadDao.updateRating(userId, "root", Rating.LIKE))
														.map($2 -> redirect302("/" +  tid));
											} catch (ParseException e) {
												return Promise.ofException(e);
											}
										})
										.thenEx(revertIfException(() -> threadDao.deleteAttachments("root", attachmentMap.keySet())))
										.thenEx(revertIfException(() -> commDao.removeThread(tid)));
							});
				})
				.map(GET, "/profile", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("profile", map(
							"shownUser", new Ref<>("user"),
							"shownUser.ip", request.getAttachment(CommDao.class).getUserLastIp(userId).map(InetAddress::getHostAddress)));
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
									templater.render("profile", map(
											"shownUser", UserView.from(commDao, shownUserId, shownUser),
											"shownUser.ip", commDao.getUserLastIp(shownUserId).map(InetAddress::getHostAddress))) :
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

					try {
						String email = getPostParameter(params, "email", 60);
						String username = getOptionalPostParameter(params, "username", 60);
						String firstName = getOptionalPostParameter(params, "firstName", 60);
						String lastName = getOptionalPostParameter(params, "lastName", 60);
						CommDao commDao = request.getAttachment(CommDao.class);
						return commDao.getUser(updatingUserId)
								.then(oldData -> {
									if (oldData == null) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
									}
									return commDao.updateUser(updatingUserId, new UserData(oldData.getRole(), email, username, firstName, lastName))
											.map($ -> redirectToReferer(request, "/"));
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
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
										try {
											String content = getPostParameter(paramsMap, "content", 4000);
											return threadDao.addPost(user, parentId, postId, content, attachmentMap)
													.then($2 -> threadDao.updateRating(user, postId, Rating.LIKE));
										} catch (ParseException e) {
											return Promise.ofException(e);
										}
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
								try {
									String content = getPostParameter(paramsMap, "content", 4000);
									Set<String> removed = Arrays.stream(nullToEmpty(paramsMap.get("removeAttachments")).split(","))
											.map(String::trim)
											.collect(toSet());
									return threadDao.updatePost(pid, content, attachmentMap, removed)
											.map($2 -> postOpRedirect(request));
								} catch (ParseException e) {
									return Promise.ofException(e);
								}
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

				.map(GET, "/users", request -> {
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.getUsers()
							.then(users -> templater.render("user_list", map("users", Promises.toList(users.entrySet().stream().map(e -> UserView.from(commDao, e.getKey(), e.getValue()))))));
				})

				.map(GET, "/user-ban/:userId", request -> {
					UserId userId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao
							.getUser(userId)
							.then(userData -> {
								if (userData == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								Map<String, Object> context = new HashMap<>();
								context.put("bannedUser", UserView.from(commDao, userId, userData));
								if (userData.getBanState() == null) {
									context.put("bannedUser.ban.until", formatInstant(Instant.now().plus(1, ChronoUnit.DAYS)));
								}
								return templater.render("user_ban", context);
							});
				})

				.map(POST, "/user-ban/:userId", request -> {
					String action;
					try {
						action = getPostParameter(request.getPostParameters(), "action", 5);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
					String userIdParam = request.getPathParameter("userId");
					UserId bannedUserId = new UserId(DK_APP_STORE, userIdParam);
					UserId userId = request.getAttachment(UserId.class);
					CommDao commDao = request.getAttachment(CommDao.class);

					return commDao.getUser(bannedUserId)
							.then(user -> {
								if (user == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								switch (action) {
									case "unban": {
										UserData newData = new UserData(user.getRole(), user.getEmail(), user.getUsername(), user.getFirstName(), user.getLastName());
										PubKey pk = commDao.getKeys().getPubKey();
										return commDao.updateUser(bannedUserId, newData)
												.map($ -> redirect302(request.getPostParameters().getOrDefault("redirect", "/" + pk.asString() + "/profile/" + userIdParam)));
									}
									case "save": {
										try {
											String reason = getPostParameter(request.getPostParameters(), "reason", 1000);
											Instant until = LocalDateTime.parse(getPostParameter(request.getPostParameters(), "until", 20), DATE_TIME_FORMAT).atZone(ZoneId.systemDefault()).toInstant();
											BanState banState = new BanState(userId, until, reason);
											UserData newData = new UserData(user.getRole(), user.getEmail(), user.getUsername(), user.getFirstName(), user.getLastName(), banState);
											PubKey pk = commDao.getKeys().getPubKey();
											return commDao.updateUser(bannedUserId, newData)
													.map($ -> redirect302(request.getPostParameters().getOrDefault("redirect", "/" + pk.asString() + "/profile/" + userIdParam)));
										} catch (ParseException e) {
											return Promise.<HttpResponse>ofException(e);
										} catch (DateTimeParseException e) {
											return Promise.<HttpResponse>ofException(new ParseException(PublicServlet.class, "Invalid datetime: " + e.getMessage()));
										}
									}
									default: {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "Unknown action '" + action + "'"));
									}
								}
							});
				})

				.map(GET, "/ip-bans", request -> templater.render("ip_bans", map("bans", IpBanView.from(request.getAttachment(CommDao.class)))))

				.map(GET, "/edit-forum", request -> templater.render("edit_forum", emptyMap()))

				.map(POST, "/edit-forum", request -> {
					try {
						Map<String, String> params = request.getPostParameters();
						String title = getPostParameter(params, "title", 60);
						String description = getPostParameter(params, "description", 1000);
						return request.getAttachment(ForumDao.class)
								.setForumMetadata(new ForumMetadata(title, description))
								.map($ -> redirect302("."));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/ip-bans/new", request -> {
					Map<String, Object> context = new HashMap<>();
					context.put("ban.id", "new");
					context.put("ban.ip", "a new range");
					String ip = request.getQueryParameter("ip");
					if (ip != null) {
						String trimmed = ip.trim();
						if (!(trimmed + ".").matches("^(?:(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5])\\.){4}$")) {
							return Promise.ofException(new ParseException(PublicServlet.class, "invalid IP: " + trimmed));
						}
						String[] parts = trimmed.split("\\.");
						for (int i = 0; i < parts.length; i++) {
							context.put("ban.ipParts.value" + (i + 1), parts[i]);
						}
					}
					context.put("ban.ban.until", Utils.formatInstant(Instant.now().plus(1, ChronoUnit.DAYS)));
					return templater.render("ip_ban", context);
				})

				.map(GET, "/ip-bans/:id", request ->
						IpBanView.from(request.getAttachment(CommDao.class), request.getPathParameter("id"))
								.then(view -> view != null ?
										templater.render("ip_ban", map("ban", view)) :
										Promise.of(redirect302("."))))

				.map(POST, "/ip-bans/:id", request -> {
					String action;
					try {
						action = getPostParameter(request.getPostParameters(), "action", 5);
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
								String reason = getPostParameter(request.getPostParameters(), "reason", 1000);
								Instant until = LocalDateTime.parse(getPostParameter(request.getPostParameters(), "until", 20), DATE_TIME_FORMAT).atZone(ZoneId.systemDefault()).toInstant();

								CommDao commDao = request.getAttachment(CommDao.class);

								String id = request.getPathParameter("id");

								Promise<Void> unban = "new".equals(id) ? Promise.complete() : commDao.unbanIpRange(id);

								return unban
										.then($ -> commDao.banIpRange(new IpRange(ip, mask), request.getAttachment(UserId.class), until, reason))
										.map($ -> redirect302("."));

							} catch (ParseException e) {
								return Promise.ofException(e);
							} catch (DateTimeParseException e) {
								return Promise.ofException(new ParseException(PublicServlet.class, "Invalid datetime: " + e.getMessage()));
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
							.then(post -> PostView.from(commDao, post, userId, role, 5))));
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
			templater.put("url.referer", request.getHeader(REFERER));
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
												templater.put("user", UserView.from(commDao, userId, user));
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
