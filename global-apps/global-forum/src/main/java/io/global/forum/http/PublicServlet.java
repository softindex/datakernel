package io.global.forum.http;

import io.datakernel.common.parse.ParseException;
import io.datakernel.common.ref.Ref;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.appstore.AppStore;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.http.AttachmentDataHandler;
import io.global.comm.pojo.*;
import io.global.common.PubKey;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.http.view.*;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.util.Utils;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;

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

import static io.datakernel.common.Utils.nullToEmpty;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpHeaders.HOST;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.http.HttpResponse.ok200;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.global.Utils.*;
import static io.global.comm.dao.ThreadDao.ATTACHMENT_NOT_FOUND;
import static io.global.comm.dao.ThreadDao.POST_NOT_FOUND;
import static io.global.comm.util.Utils.generateId;
import static io.global.forum.util.Utils.*;
import static io.global.ot.session.AuthService.DK_APP_STORE;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

public final class PublicServlet {
	private static final String SESSION_ID = "FORUM_SID";
	private static final int IP_BANS_PAGE_LIMIT = 5;
	private static final int USERS_PAGE_LIMIT = 10;
	private static final int THREADS_PAGE_LIMIT = 10;

	public static final int MAX_DEPTH = 2;

	public static AsyncServlet create(String appStoreUrl, AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", threadListServlet(templater))
				.merge(authorizationServlet(appStore, templater))
				.merge(newThreadServlet(templater))
				.map("/admin/*", adminServlet(templater))
				.map("/profile/*", profileServlet(templater))
				.map("/:threadID/*", threadServlet(templater))

				.then(session(templater))
				.then(setup(appStoreUrl, templater));
	}

	private static AsyncServlet threadListServlet(MustacheTemplater templater) {
		return request -> {
			CommDao commDao = request.getAttachment(CommDao.class);
			UserId userId = request.getAttachment(UserId.class);

			int page = getUnsignedInt(request, "p", 0);

			return commDao.getThreads("root").size()
					.then(all -> {
						int lastPage = (all + THREADS_PAGE_LIMIT - 1) / THREADS_PAGE_LIMIT;
						if (page > lastPage) {
							return Promise.of(redirect302("?p=" + lastPage));
						}
						return ThreadView.from(commDao, page, THREADS_PAGE_LIMIT, userId, request.getAttachment(UserRole.class), -1)
								.then(threads ->
										templater.render("thread_list", map(
												"threads", threads,
												"pages", all > THREADS_PAGE_LIMIT ? new PageView(page, THREADS_PAGE_LIMIT, all) : null,
												"threads.any", threads.isEmpty() ? null : threads.get(0)
												),
												isGzipAccepted(request)));
					});

		};
	}

	private static RoutingServlet authorizationServlet(AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
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
					return templater.render("login", map("loginScreen", true, "origin", origin), isGzipAccepted(request));
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
										.getUsers().get(userId)
										.then(existing -> {
											if (existing != null) {
												return Promise.complete();
											}
											UserRole userRole = containerPubKey.equals(pubKey) ?
													UserRole.OWNER :
													UserRole.COMMON;

											UserData userData = new UserData(userRole, profile.getEmail(),
													profile.getUsername(), profile.getFirstName(), profile.getLastName(), null);
											return commDao.getUsers().put(userId, userData);
										})
										.then($ -> {
											SessionStore<UserId> sessionStore = commDao.getSessionStore();
											return sessionStore.save(sessionId, userId)
													.map($2 -> {
														String origin = request.getQueryParameter("origin");
														HttpCookie sessionCookie = HttpCookie.of(SESSION_ID, sessionId)
																.withPath("/");
														Duration lifetimeHint = sessionStore.getSessionLifetimeHint();
														if (lifetimeHint != null) {
															sessionCookie.setMaxAge(lifetimeHint);
														}
														return redirect302(origin != null ? origin : "/")
																.withCookie(sessionCookie);
													});
										});
							});
				})
				.map(POST, "/logout", request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(ok200());
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> redirectToReferer(request, "/")
									.withCookie(HttpCookie.of(SESSION_ID, "")
											.withPath("/")
											.withMaxAge(Duration.ZERO)));
				});
	}

	private static RoutingServlet newThreadServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/new", request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("new_thread", emptyMap(), isGzipAccepted(request));
				})
				.map(POST, "/new", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					CommDao commDao = request.getAttachment(CommDao.class);

					return commDao.generateThreadId()
							.then(id -> commDao.getThreads("root").put(id, ThreadMetadata.of("", 0)).map($ -> id))
							.then(tid -> commDao.getThreadDao(tid)
									.then(threadDao -> {
										assert threadDao != null : "No thread dao just after creating the thread";

										Map<String, AttachmentType> attachmentMap = new HashMap<>();
										Map<String, String> paramsMap = new HashMap<>();

										return request.handleMultipart(AttachmentDataHandler.create(threadDao, "root", paramsMap, attachmentMap, true))
												.then($ -> {
													try {
														String title = getPostParameter(paramsMap, "title", 120);
														String content = getPostParameter(paramsMap, "content", 4000);

														return commDao.getThreads("root").put(tid, ThreadMetadata.of(title, Instant.now().toEpochMilli()))
																.then($2 -> threadDao.addRootPost(userId, content, attachmentMap))
																.then($2 -> threadDao.updateRating(userId, "root", Rating.LIKE))
																.map($2 -> redirect302("/" + tid));
													} catch (ParseException e) {
														return Promise.ofException(e);
													}
												})
												.thenEx(revertIfException(() -> threadDao.deleteAttachments("root", attachmentMap.keySet())))
												.thenEx(revertIfException(() -> commDao.getThreads("root").remove(tid)));
									}));
				});
	}

	private static AsyncServlet profileServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("profile", map(
							"shownUser", new Ref<>("user"),
							"shownUser.ip", request.getAttachment(CommDao.class).getUserLastIps().get(userId).map(InetAddress::getHostAddress)),
							isGzipAccepted(request));
				})
				.map(GET, "/:userId", request -> {
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
							.getUsers().get(shownUserId)
							.then(shownUser -> shownUser != null ?
									templater.render("profile", map(
											"shownUser", UserView.from(commDao, shownUserId, shownUser),
											"shownUser.ip", commDao.getUserLastIps().get(shownUserId).map(InetAddress::getHostAddress)),
											isGzipAccepted(request)) :
									Promise.ofException(HttpException.ofCode(400, "No such user")));
				})
				.map(POST, "/:userId", loadBody().serve(request -> {
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
						String firstName = getOptionalPostParameter(params, "first_name", 60);
						String lastName = getOptionalPostParameter(params, "last_name", 60);
						CommDao commDao = request.getAttachment(CommDao.class);
						return commDao.getUsers().get(updatingUserId)
								.then(oldData -> {
									if (oldData == null) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
									}
									return commDao.getUsers().put(updatingUserId, new UserData(oldData.getRole(), email, username, firstName, lastName, oldData.getBanState()))
											.map($ -> redirectToReferer(request, "/"));
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				}));
	}

	private static AsyncServlet threadServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", postViewServlet(templater))
				// merge here is because parameters (/:postID) have more priority than fallbacks (/*)
				.merge(threadOperations())
				.map(GET, "/:postID/", postViewServlet(templater))
				.map("/:postID/*", postOperations(templater))
				.then(attachThreadDao());
	}

	private static RoutingServlet threadOperations() {
		return RoutingServlet.create()
				.map(POST, "/rename", loadBody().serve(request -> {
					String tid = request.getPathParameter("threadID");
					CommDao commDao = request.getAttachment(CommDao.class);
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					UserId userId = request.getAttachment(UserId.class);
					UserRole userRole = request.getAttachment(UserRole.class);

					return threadDao.getPost("root")
							.then(post -> {
								if (!userRole.isPrivileged() && !post.getAuthor().equals(userId)) {
									return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
								}
								if (post == null) {
									return Promise.ofException(HttpException.ofCode(500, "No root post"));
								}
								try {
									String title = getPostParameter(request.getPostParameters(), "title", 120);
									return threadDao.getThreadMetadata()
											.then(threadMeta -> commDao.getThreads("root").put(tid, ThreadMetadata.of(title, threadMeta.getLastUpdate())));
								} catch (ParseException e) {
									return Promise.ofException(e);
								}
							})
							.map($2 -> redirect302("/" + tid));
				}))
				.map(DELETE, "/", request -> {
					String tid = request.getPathParameter("threadID");
					CommDao commDao = request.getAttachment(CommDao.class);
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					UserId userId = request.getAttachment(UserId.class);
					UserRole userRole = request.getAttachment(UserRole.class);

					return threadDao.getPost("root")
							.then(post -> {
								if (!userRole.isPrivileged() && !post.getAuthor().equals(userId)) {
									return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
								}
								return commDao.getThreads("root").remove(tid);
							})
							.map($2 -> redirect302("/" + tid));
				});
	}

	private static HttpResponse redirectToLogin(HttpRequest request) {
		return redirect302("/login?origin=" + request.getPath());
	}

	private static Promise<HttpResponse> postOpPartialReply(HttpRequest request, ThreadDao threadDao, String pid, MustacheTemplater templater, boolean partial) {
		String tid = request.getPathParameter("threadID");
		CommDao commDao = request.getAttachment(CommDao.class);
		UserId userId = request.getAttachment(UserId.class);
		UserRole userRole = request.getAttachment(UserRole.class);
		return templater.render(partial ? "render_post_body" : "render_post", map(
				".", threadDao.getPost(pid).then(post -> PostView.single(commDao, post, userId, userRole, MAX_DEPTH)),
				"thread", ThreadView.root(commDao, tid, userId, userRole)

				),
				isGzipAccepted(request));
	}

	private static AsyncServlet postOperations(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(POST, "/", request -> {
					CommDao commDao = request.getAttachment(CommDao.class);
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					Map<String, AttachmentType> attachmentMap = new HashMap<>();
					Map<String, String> paramsMap = new HashMap<>();

					UserId user = request.getAttachment(UserId.class);
					String threadId = request.getPathParameter("threadID");
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
									})
									.thenEx(revertIfException(() -> threadDao.deleteAttachments(parentId, attachmentMap.keySet())))
									.then($ -> commDao.getThreads("root").get(threadId))
									.then(thread -> {
										if (thread == null) {
											return Promise.of(POST_NOT_FOUND);
										}
										return commDao.getThreads("root").put(threadId, thread.updated(Instant.now().toEpochMilli()));
									})
									.then($ -> postOpPartialReply(request, threadDao, postId, templater, false)));
				})
				.map(POST, "/rate/:rating", request -> {
					String pid = request.getPathParameter("postID");
					return request.getAttachment(ThreadDao.class)
							.updateRating(request.getAttachment(UserId.class), pid, Rating.fromString(request.getPathParameter("rating")))
							.map($ -> ok200());
				})
				.map(POST, "/delete", request -> {
					String pid = request.getPathParameter("postID");
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					UserId userId = request.getAttachment(UserId.class);
					UserRole userRole = request.getAttachment(UserRole.class);

					return threadDao.getPost(pid)
							.then(post -> {
								if (!userRole.isPrivileged() && !post.getAuthor().equals(userId)) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(403, "Not privileged"));
								}
								return threadDao.removePost(userId, pid);
							})
							.then($ -> postOpPartialReply(request, threadDao, pid, templater, true));
				})
				.map(POST, "/restore", request -> {
					String pid = request.getPathParameter("postID");
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					UserId userId = request.getAttachment(UserId.class);
					UserRole userRole = request.getAttachment(UserRole.class);

					return threadDao.getPost(pid)
							.then(post -> {
								if (!userRole.isPrivileged() && !post.getAuthor().equals(userId)) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(403, "Not privileged"));
								}
								return threadDao
										.restorePost(pid);
							})
							.then($ -> postOpPartialReply(request, threadDao, pid, templater, true));
				})
				.map(POST, "/edit", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);

					String pid = request.getPathParameter("postID");
					UserId userId = request.getAttachment(UserId.class);
					UserRole userRole = request.getAttachment(UserRole.class);

					Map<String, AttachmentType> attachmentMap = new HashMap<>();
					Map<String, String> paramsMap = new HashMap<>();

					return threadDao.getPost(pid)
							.then(post -> {
								if (!userRole.isPrivileged() && !post.getAuthor().equals(userId)) {
									return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
								}
								return threadDao.listAttachments(pid)
										.then(existing -> request.handleMultipart(AttachmentDataHandler.create(threadDao, pid, existing, paramsMap, attachmentMap, true)))
										.then($ -> {
											try {
												String content = getPostParameter(paramsMap, "content", 4000);
												Set<String> removed = Arrays.stream(nullToEmpty(paramsMap.get("removeAttachments")).split(","))
														.map(String::trim)
														.collect(toSet());
												return threadDao.updatePost(pid, content, attachmentMap, removed);
											} catch (ParseException e) {
												return Promise.ofException(e);
											}
										})
										.thenEx(revertIfException(() -> threadDao.deleteAttachments(pid, attachmentMap.keySet())))
										.map($ -> ok200());
							})
							.then($ -> postOpPartialReply(request, threadDao, pid, templater, true));
				})
				.map(GET, "/download/:filename", cachedContent().serve(request -> {
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
											request.getHeader(HttpHeaders.RANGE),
											request.getQueryParameter("inline") != null
									);
								} else if (e == ATTACHMENT_NOT_FOUND) {
									return Promise.of(HttpResponse.notFound404());
								} else {
									return Promise.<HttpResponse>ofException(e);
								}
							});
				}))
				.then(servlet -> request -> {
					if (request.getMethod() == POST) {
						if (request.getAttachment(UserId.class) == null) {
							return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
						}
					}
					return servlet.serve(request);
				});
	}

	private static AsyncServlet adminServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> templater.render("admin_panel", isGzipAccepted(request)))

				.map(GET, "/edit-forum", request -> templater.render("edit_forum", emptyMap(), isGzipAccepted(request)))

				.map(POST, "/edit-forum", request -> {
					try {
						Map<String, String> params = request.getPostParameters();
						String title = getOptionalPostParameter(params, "title", 60);
						String description = getPostParameter(params, "description", 1000);
						return request.getAttachment(ForumDao.class)
								.setForumMetadata(new ForumMetadata(title, description))
								.map($ -> redirect302("."));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})

				.map(GET, "/ip-bans", request -> {
					CommDao commDao = request.getAttachment(CommDao.class);
					int page = getUnsignedInt(request, "p", 0);
					return commDao.getIpBans().size()
							.then(all ->
									IpBanView.from(commDao, page, IP_BANS_PAGE_LIMIT)
											.then(bans ->
													templater.render("ip_bans", map(
															"bans", bans,
															"pages", all > IP_BANS_PAGE_LIMIT ? new PageView(page, IP_BANS_PAGE_LIMIT, all) : null,
															"bans.any", bans.isEmpty() ? null : bans.get(0)),
															isGzipAccepted(request))));
				})

				.map(GET, "/ip-bans/new", request -> {
					Map<String, Object> context = new HashMap<>();
					context.put("ban.banId", "new");
					context.put("ban.ip", "a new range");
					String ip = request.getQueryParameter("ip");
					if (ip != null) {
						String trimmed = ip.trim();
						if (!(trimmed + ".").matches("^(?:(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5])\\.){4}$")) {
							return Promise.ofException(new ParseException(PublicServlet.class, "invalid IP: " + trimmed + ", only valid IPv4's are allowed"));
						}
						String[] parts = trimmed.split("\\.");
						for (int i = 0; i < parts.length; i++) {
							context.put("ban.ipParts.value" + (i + 1), parts[i]);
						}
					}
					context.put("ban.ban.until", Utils.formatInstant(Instant.now().plus(1, ChronoUnit.DAYS)));
					return templater.render("ip_ban", context, isGzipAccepted(request));
				})

				.map(GET, "/ip-bans/:id", request ->
						IpBanView.from(request.getAttachment(CommDao.class), request.getPathParameter("id"))
								.then(view -> view != null ?
										templater.render("ip_ban", map("ban", view), isGzipAccepted(request)) :
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
									.getIpBans().remove(id)
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

								Promise<Void> unban = "new".equals(id) ? Promise.complete() : commDao.getIpBans().remove(id);

								return unban
										.then($ -> {
											BanState ban = new BanState(request.getAttachment(UserId.class), until, reason);
											IpBanState ipBan = new IpBanState(ban, new IpRange(ip, mask));
											return commDao.getIpBans().put(generateId(), ipBan);
										})
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

				.map(GET, "/users", request -> {
					CommDao commDao = request.getAttachment(CommDao.class);
					int page = getUnsignedInt(request, "p", 0);
					return commDao.getUsers().size()
							.then(all ->
									UserView.from(commDao, page, USERS_PAGE_LIMIT)
											.then(users ->
													templater.render("user_list", map(
															"users", users,
															"pages", all > USERS_PAGE_LIMIT ? new PageView(page, USERS_PAGE_LIMIT, all) : null,
															"users.any", users.isEmpty() ? null : users.get(0)),
															isGzipAccepted(request))));
				})

				.map(GET, "/user-ban/:userId", request -> {
					UserId userId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao
							.getUsers().get(userId)
							.then(userData -> {
								if (userData == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								Map<String, Object> context = new HashMap<>();
								context.put("bannedUser", UserView.from(commDao, userId, userData));
								if (userData.getBanState() == null || userData.getBanState().getUntil().compareTo(Instant.now()) < 0) {
									context.put("bannedUser.ban.until", formatInstant(Instant.now().plus(1, ChronoUnit.DAYS)));
								}
								return templater.render("user_ban", context, isGzipAccepted(request));
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

					return commDao.getUsers().get(bannedUserId)
							.then(user -> {
								if (user == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								switch (action) {
									case "unban": {
										UserData newData = new UserData(user.getRole(), user.getEmail(), user.getUsername(), user.getFirstName(), user.getLastName());
										PubKey pk = commDao.getKeys().getPubKey();
										return commDao.getUsers().put(bannedUserId, newData)
												.map($ -> redirect302(request.getPostParameters().getOrDefault("redirect", "/" + pk.asString() + "/profile/" + userIdParam)));
									}
									case "save": {
										try {
											String reason = getPostParameter(request.getPostParameters(), "reason", 1000);
											Instant until = LocalDateTime.parse(getPostParameter(request.getPostParameters(), "until", 20), DATE_TIME_FORMAT).atZone(ZoneId.systemDefault()).toInstant();
											BanState banState = new BanState(userId, until, reason);
											UserData newData = new UserData(user.getRole(), user.getEmail(), user.getUsername(), user.getFirstName(), user.getLastName(), banState);
											PubKey pk = commDao.getKeys().getPubKey();
											return commDao.getUsers().put(bannedUserId, newData)
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

				.then(servlet ->
						request -> {
							if (request.getAttachment(UserId.class) == null) {
								return Promise.of(redirectToLogin(request));
							}
							if (!request.getAttachment(UserRole.class).isPrivileged()) {
								return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
							}
							return servlet.serve(request);
						})
				.then(loadBody());
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

			if (pid == null) {
				return templater.render("thread", map("thread", ThreadView.from(commDao, tid, userId, role, MAX_DEPTH)), isGzipAccepted(request));
			}
			return templater.render("subthread", map(
					"thread", ThreadView.root(commDao, tid, userId, role),
					"post", threadDao.getPost(pid).then(post -> PostView.from(commDao, post, userId, role, MAX_DEPTH))
					),
					isGzipAccepted(request));
		};
	}

	private static AsyncServletDecorator setup(String appStoreUrl, MustacheTemplater templater) {
		return onRequest(request -> {
			ForumDao forumDao = request.getAttachment(ForumUserContainer.class).getForumDao();
			request.attach(ForumDao.class, forumDao);
			request.attach(CommDao.class, forumDao.getCommDao());

			templater.clear();
			templater.put("appStoreUrl", appStoreUrl);

			String host = request.getHeader(HOST);
			if (host == null) {
				host = request.getHostAndPort();
			}
			assert host != null : "host should not be null here";

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
									maxAge = Promises.toTuple(commDao.getUserLastIps().put(userId, request.getRemoteAddress()), commDao.getUsers().get(userId))
											.map(t -> {
												UserData user = t.getValue2();
												templater.put("user", UserView.from(commDao, userId, user));
												request.attach(userId);
												request.attach(user);
												request.attach(user.getRole());
												return sessionStore.getSessionLifetimeHint();
											});
								} else {
									request.attach(UserRole.GUEST);
									maxAge = Promise.of(Duration.ZERO);
								}
								return maxAge.then(age ->
										servlet.serve(request).get()
												.map(response -> {
													if (response.getCookie(SESSION_ID) != null) { // servlet itself had set the session (logout request)
														return response;
													}
													HttpCookie sessionCookie = HttpCookie.of(SESSION_ID, sessionId)
															.withPath("/");
													if (age != null) {
														sessionCookie.setMaxAge(age);
													}
													return response
															.withCookie(sessionCookie);
												}));
							});
				};
	}

	private static AsyncServletDecorator attachThreadDao() {
		return servlet ->
				request -> {
					String id = request.getPathParameter("threadID");
					return request.getAttachment(CommDao.class).getThreadDao(id)
							.then(dao -> {
								if (dao == null) {
									return Promise.ofException(HttpException.ofCode(404, "No thread with id " + id));
								}
								request.attach(ThreadDao.class, dao);
								return servlet.serve(request).get();
							});
				};
	}
}
