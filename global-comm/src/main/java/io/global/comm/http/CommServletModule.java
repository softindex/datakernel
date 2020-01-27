package io.global.comm.http;

import io.datakernel.common.parse.ParseException;
import io.datakernel.common.ref.Ref;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.http.*;
import io.datakernel.http.di.RouterModule;
import io.datakernel.http.di.RouterModule.Mapped;
import io.datakernel.http.di.RouterModule.Router;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.appstore.AppStore;
import io.global.comm.container.CommUserContainer;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.http.view.*;
import io.global.comm.ot.AppMetadata;
import io.global.comm.pojo.*;
import io.global.common.PubKey;
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
import static io.datakernel.http.HttpResponse.ok200;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.http.di.RouterModule.MappedHttpMethod.*;
import static io.global.Utils.*;
import static io.global.comm.dao.ThreadDao.ATTACHMENT_NOT_FOUND;
import static io.global.comm.dao.ThreadDao.POST_NOT_FOUND;
import static io.global.comm.util.Utils.*;
import static io.global.ot.session.AuthService.DK_APP_STORE;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

public final class CommServletModule extends AbstractModule {
	private final String sessionCookieName;
	private final int ipBansPageLimit;
	private final int usersPageLimit;
	private final int threadsPageLimit;
	private final int maxDepth;

	private CommServletModule(String sessionCookieName, int ipBansPageLimit, int usersPageLimit, int threadsPageLimit, int maxDepth) {
		this.sessionCookieName = sessionCookieName;
		this.ipBansPageLimit = ipBansPageLimit;
		this.usersPageLimit = usersPageLimit;
		this.threadsPageLimit = threadsPageLimit;
		this.maxDepth = maxDepth;
	}

	public static Module create(String sessionCookieName, int ipBansPageLimit, int usersPageLimit, int threadsPageLimit, int maxDepth) {
		return new CommServletModule(sessionCookieName, ipBansPageLimit, usersPageLimit, threadsPageLimit, maxDepth);
	}

	@Override
	protected void configure() {
		install(new RouterModule());
	}

	@Provides
	PostRenderer defaultPostRenderer() {
		return ($, post) -> post.getContent();
	}

	@Provides
	PrivilegePredicate defaultPrivilegePredicate() {
		return request -> !request.getPath().startsWith("/admin");
	}

	@Provides
	AsyncServlet root(Config config, AppStore appStore, MustacheTemplater templater, PrivilegePredicate predicate, @Router AsyncServlet root) {
		return root
				.then(loadBody())
				.then(checkPrivileges(predicate))
				.then(session(templater, sessionCookieName))
				.then(setup(config.get("appStoreUrl"), templater))
				.then(gzip());
	}

	@Provides
	@Mapped(value = "/", method = GET)
	AsyncServlet index(AppStore appStore, MustacheTemplater templater, PostRenderer postRenderer) {
		return request -> {
			CommDao commDao = request.getAttachment(CommDao.class);
			UserId userId = request.getAttachment(UserId.class);

			int page = getUnsignedInt(request, "p", 0);

			return commDao.getThreads("root").size()
					.then(all -> {
						int lastPage = (all + threadsPageLimit - 1) / threadsPageLimit;
						if (page > lastPage) {
							return Promise.of(redirect302("?p=" + lastPage));
						}
						return ThreadView.from(commDao, page, threadsPageLimit, userId, request.getAttachment(UserRole.class), -1, postRenderer)
								.then(threads ->
										templater.render("thread_list", map(
												"threads", threads,
												"pages", all > threadsPageLimit ? new PageView(page, threadsPageLimit, all) : null,
												"threads.any", threads.isEmpty() ? null : threads.get(0)
										)));
					});

		};
	}

	@Provides
	@Mapped(value = "/login", method = GET)
	AsyncServlet login(AppStore appStore, MustacheTemplater templater) {
		return request -> {
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
		};
	}

	@Provides
	@Mapped(value = "/authorize", method = GET)
	AsyncServlet authorize(AppStore appStore) {
		return request -> {
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
												HttpCookie sessionCookie = HttpCookie.of(sessionCookieName, sessionId)
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
		};
	}

	@Provides
	@Mapped(value = "/logout", method = POST)
	AsyncServlet logout() {
		return request -> {
			String sessionId = request.getCookie(sessionCookieName);
			if (sessionId == null) {
				return Promise.of(ok200());
			}
			CommDao commDao = request.getAttachment(CommDao.class);
			return commDao.getSessionStore().remove(sessionId)
					.map($ -> redirectToReferer(request, "/")
							.withCookie(HttpCookie.of(sessionCookieName, "")
									.withPath("/")
									.withMaxAge(Duration.ZERO)));
		};
	}

	@Provides
	@Mapped(value = "/new", method = GET)
	AsyncServlet renderNew(MustacheTemplater templater) {
		return request -> {
			if (request.getAttachment(UserId.class) == null) {
				return Promise.of(redirectToLogin(request));
			}
			return templater.render("new_thread", emptyMap());
		};
	}

	@Provides
	@Mapped(value = "/new", method = POST)
	AsyncServlet postNew() {
		return request -> {
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
		};
	}

	@Provides
	@Mapped(value = "/profile", method = GET)
	AsyncServlet renderOwnProfile(MustacheTemplater templater) {
		return request -> {
			UserId userId = request.getAttachment(UserId.class);
			if (userId == null) {
				return Promise.of(redirectToLogin(request));
			}
			return templater.render("profile", map(
					"shownUser", new Ref<>("user"),
					"shownUser.ip", request.getAttachment(CommDao.class).getUserLastIps().get(userId).map(InetAddress::getHostAddress)));
		};
	}

	@Provides
	@Mapped(value = "/profile/:userId", method = GET)
	AsyncServlet renderProfile(MustacheTemplater templater) {
		return request -> {
			UserId userId = request.getAttachment(UserId.class);
			if (userId == null) {
				return Promise.of(redirectToLogin(request));
			}
			if (!request.getAttachment(UserRole.class).isPrivileged()) {
				return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
			}
			CommDao commDao = request.getAttachment(CommDao.class);
			UserId shownUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
			return commDao.getUsers().get(shownUserId)
					.then(shownUser -> shownUser != null ?
							templater.render("profile", map(
									"shownUser", UserView.from(commDao, shownUserId, shownUser),
									"shownUser.ip", commDao.getUserLastIps().get(shownUserId).map(InetAddress::getHostAddress))) :
							Promise.ofException(HttpException.ofCode(400, "No such user")));
		};
	}

	@Provides
	@Mapped(value = "/profile/:userId", method = POST)
	AsyncServlet updateProfile() {
		return loadBody().serve(request -> {
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
		});
	}

	@Provides
	@Mapped(value = "/admin", method = GET)
	AsyncServlet renderAdminPanel(MustacheTemplater templater) {
		return request -> templater.render("admin_panel");
	}

	@Provides
	@Mapped(value = "/admin/edit", method = GET)
	AsyncServlet renderEdit(MustacheTemplater templater) {
		return request -> templater.render("edit", emptyMap());
	}

	@Provides
	@Mapped(value = "/admin/edit", method = POST)
	AsyncServlet edit() {
		return request -> {
			try {
				Map<String, String> params = request.getPostParameters();
				String title = getOptionalPostParameter(params, "title", 60);
				String description = getOptionalPostParameter(params, "description", 1000);
				return request.getAttachment(CommDao.class)
						.setAppMetadata(new AppMetadata(title, description))
						.map($ -> redirect302("."));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	@Provides
	@Mapped(value = "/admin/ip-bans", method = GET)
	AsyncServlet renderIpBans(MustacheTemplater templater) {
		return request -> {
			CommDao commDao = request.getAttachment(CommDao.class);
			int page = getUnsignedInt(request, "p", 0);
			return commDao.getIpBans().size()
					.then(all ->
							IpBanView.from(commDao, page, ipBansPageLimit)
									.then(bans ->
											templater.render("ip_bans", map(
													"bans", bans,
													"pages", all > ipBansPageLimit ? new PageView(page, ipBansPageLimit, all) : null,
													"bans.any", bans.isEmpty() ? null : bans.get(0)))));
		};
	}

	@Provides
	@Mapped(value = "/admin/ip-bans/new", method = GET)
	AsyncServlet renderNewIpBan(MustacheTemplater templater) {
		return request -> {
			Map<String, Object> context = new HashMap<>();
			context.put("ban.banId", "new");
			context.put("ban.ip", "a new range");
			String ip = request.getQueryParameter("ip");
			if (ip != null) {
				String trimmed = ip.trim();
				if (!(trimmed + ".").matches("^(?:(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|2s5[0-5])\\.){4}$")) {
					return Promise.ofException(new ParseException(CommServletModule.class, "invalid IP: " + trimmed + ", only valid IPv4's are allowed"));
				}
				String[] parts = trimmed.split("\\.");
				for (int i = 0; i < parts.length; i++) {
					context.put("ban.ipParts.value" + (i + 1), parts[i]);
				}
			}
			context.put("ban.ban.until", formatInstant(Instant.now().plus(1, ChronoUnit.DAYS)));
			return templater.render("ip_ban", context);
		};
	}

	@Provides
	@Mapped(value = "/admin/ip-bans/:id", method = GET)
	AsyncServlet renderIpBan(MustacheTemplater templater) {
		return request ->
				IpBanView.from(request.getAttachment(CommDao.class), request.getPathParameter("id"))
						.then(view -> view != null ?
								templater.render("ip_ban", map("ban", view)) :
								Promise.of(redirect302(".")));
	}

	@Provides
	@Mapped(value = "/admin/ip-bans/:id", method = POST)
	AsyncServlet postIpBan() {
		return request -> {
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
						return Promise.ofException(new ParseException(CommServletModule.class, "Invalid datetime: " + e.getMessage()));
					}
				}
				default: {
					return Promise.ofException(HttpException.ofCode(400, "Unknown action '" + action + "'"));
				}
			}
		};
	}

	@Provides
	@Mapped(value = "/admin/users", method = GET)
	AsyncServlet renderUsers(MustacheTemplater templater) {
		return request -> {
			CommDao commDao = request.getAttachment(CommDao.class);
			int page = getUnsignedInt(request, "p", 0);
			return commDao.getUsers().size()
					.then(all ->
							UserView.from(commDao, page, usersPageLimit)
									.then(users ->
											templater.render("user_list", map(
													"users", users,
													"pages", all > usersPageLimit ? new PageView(page, usersPageLimit, all) : null,
													"users.any", users.isEmpty() ? null : users.get(0)))));
		};
	}

	@Provides
	@Mapped(value = "/admin/user-ban/:userId", method = GET)
	AsyncServlet renderUserBan(MustacheTemplater templater) {
		return request -> {
			UserId userId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
			CommDao commDao = request.getAttachment(CommDao.class);
			return commDao
					.getUsers().get(userId)
					.then(userData -> {
						if (userData == null) {
							return Promise.ofException(HttpException.ofCode(400, "No such user"));
						}
						Map<String, Object> context = new HashMap<>();
						context.put("bannedUser", UserView.from(commDao, userId, userData));
						if (userData.getBanState() == null || userData.getBanState().getUntil().compareTo(Instant.now()) < 0) {
							context.put("bannedUser.ban.until", formatInstant(Instant.now().plus(1, ChronoUnit.DAYS)));
						}
						return templater.render("user_ban", context);
					});
		};
	}

	@Provides
	@Mapped(value = "/admin/user-ban/:userId", method = POST)
	AsyncServlet postUserBan() {
		return request -> {
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
							return Promise.ofException(HttpException.ofCode(400, "No such user"));
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
									return Promise.ofException(e);
								} catch (DateTimeParseException e) {
									return Promise.ofException(new ParseException(CommServletModule.class, "Invalid datetime: " + e.getMessage()));
								}
							}
							default: {
								return Promise.ofException(HttpException.ofCode(400, "Unknown action '" + action + "'"));
							}
						}
					});
		};
	}

	@Provides
	@Mapped(value = "/:threadID", method = GET)
	AsyncServlet renderRootPost(MustacheTemplater templater, PostRenderer postRenderer) {
		return postViewServlet(templater, postRenderer);
	}

	@Provides
	@Mapped(value = "/:threadID/:postId", method = GET)
	AsyncServlet renderPost(MustacheTemplater templater, PostRenderer postRenderer) {
		return postViewServlet(templater, postRenderer);
	}

	@Provides
	@Mapped(value = "/:threadID/:postID", method = POST)
	AsyncServlet newPost(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao()
				.serve(request -> {
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
									.then($ -> postOpPartialReply(request, threadDao, postId, templater, postRenderer, false)));
				});
	}

	@Provides
	@Mapped(value = "/:threadID/:postID/rate/:rating", method = POST)
	AsyncServlet updatePostRating(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().serve(request -> {
			String pid = request.getPathParameter("postID");
			return request.getAttachment(ThreadDao.class)
					.updateRating(request.getAttachment(UserId.class), pid, Rating.fromString(request.getPathParameter("rating")))
					.map($ -> ok200());
		});
	}

	@Provides
	@Mapped(value = "/:threadID/:postID/delete", method = POST)
	AsyncServlet deletePost(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().serve(request -> {
			String pid = request.getPathParameter("postID");
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);
			UserId userId = request.getAttachment(UserId.class);
			UserRole userRole = request.getAttachment(UserRole.class);

			return threadDao.getPost(pid)
					.then(post -> userRole.isPrivileged() || post.getAuthor().equals(userId) ?
							threadDao.removePost(userId, pid) :
							Promise.ofException(HttpException.ofCode(403, "Not privileged")))
					.then($ -> postOpPartialReply(request, threadDao, pid, templater, postRenderer, true));
		});
	}

	@Provides
	@Mapped(value = "/:threadID/:postID/restore", method = POST)
	AsyncServlet restorePost(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().serve(request -> {
			String pid = request.getPathParameter("postID");
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);
			UserId userId = request.getAttachment(UserId.class);
			UserRole userRole = request.getAttachment(UserRole.class);

			return threadDao.getPost(pid)
					.then(post -> userRole.isPrivileged() || post.getAuthor().equals(userId) ?
							threadDao.restorePost(pid) :
							Promise.ofException(HttpException.ofCode(403, "Not privileged")))
					.then($ -> postOpPartialReply(request, threadDao, pid, templater, postRenderer, true));
		});
	}

	public static Promise<Void> handleAttachments(HttpRequest request, String postId) {
		ThreadDao threadDao = request.getAttachment(ThreadDao.class);

		Map<String, AttachmentType> attachmentMap = new HashMap<>();
		Map<String, String> paramsMap = new HashMap<>();

		return threadDao.listAttachments(postId)
				.then(existing -> request.handleMultipart(AttachmentDataHandler.create(threadDao, postId, existing, paramsMap, attachmentMap, true)))
				.then($ -> {
					try {
						String content = getPostParameter(paramsMap, "content", 4000);
						Set<String> removed = Arrays.stream(nullToEmpty(paramsMap.get("removeAttachments")).split(","))
								.map(String::trim)
								.collect(toSet());
						return threadDao.updatePost(postId, content, attachmentMap, removed);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.thenEx(revertIfException(() -> threadDao.deleteAttachments(postId, attachmentMap.keySet())));
	}

	@Provides
	@Mapped(value = "/:threadID/:postID/edit", method = POST)
	AsyncServlet editPost(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().serve(request -> {
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);
			String postId = request.getPathParameter("postID");
			UserId userId = request.getAttachment(UserId.class);
			UserRole userRole = request.getAttachment(UserRole.class);
			return threadDao.getPost(postId)
					.then(post -> userRole.isPrivileged() || post.getAuthor().equals(userId) ?
							handleAttachments(request, postId) :
							Promise.ofException(HttpException.ofCode(403, "Not privileged")))
					.then($ -> postOpPartialReply(request, threadDao, postId, templater, postRenderer, true));
		});
	}

	@Provides
	@Mapped(value = "/:threadID/:postID/download/:filename", method = GET)
	AsyncServlet downloadAttachment(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().then(cachedContent()).serve(request -> {
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
		});
	}

	@Provides
	@Mapped(value = "/:threadID/rename", method = POST)
	AsyncServlet renameThread() {
		return attachThreadDao().serve(request -> {
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
		});
	}

	@Provides
	@Mapped(value = "/:threadID", method = DELETE)
	AsyncServlet deleteThread(MustacheTemplater templater) {
		return attachThreadDao().serve(request -> {
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

	private AsyncServlet postViewServlet(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().serve(request -> {
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
				return templater.render("thread", map("thread", ThreadView.from(commDao, tid, userId, role, maxDepth, postRenderer)));
			}
			return templater.render("subthread", map(
					"thread", ThreadView.root(commDao, tid, userId, role, postRenderer),
					"post", threadDao.getPost(pid).then(post -> PostView.from(commDao, tid, post, userId, role, maxDepth, postRenderer))
			));
		});
	}

	public static HttpResponse redirectToLogin(HttpRequest request) {
		return redirect302("/login?origin=" + request.getPath());
	}

	private Promise<HttpResponse> postOpPartialReply(HttpRequest request, ThreadDao threadDao, String pid, MustacheTemplater templater, PostRenderer postRenderer, boolean partial) {
		String tid = request.getPathParameter("threadID");
		CommDao commDao = request.getAttachment(CommDao.class);
		UserId userId = request.getAttachment(UserId.class);
		UserRole userRole = request.getAttachment(UserRole.class);
		return templater.render("render_post" + (partial ? "_body" : ""), map(
				".", threadDao.getPost(pid).then(post -> PostView.single(commDao, tid, post, userId, userRole, maxDepth, postRenderer)),
				"thread", ThreadView.root(commDao, tid, userId, userRole, postRenderer)));
	}

	public static AsyncServletDecorator attachThreadDao() {
		return servlet ->
				request -> {
					if (request.getMethod() == HttpMethod.POST && request.getAttachment(UserId.class) == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
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

	public static AsyncServletDecorator checkPrivileges(PrivilegePredicate predicate) {
		return servlet ->
				request -> {
					if (predicate.isAllowedWithNoPrivilege(request)) {
						return servlet.serve(request);
					}
					if (request.getAttachment(UserId.class) == null) {
						return Promise.of(redirectToLogin(request));
					}
					if (!request.getAttachment(UserRole.class).isPrivileged()) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}
					return servlet.serve(request);
				};
	}

	public static AsyncServletDecorator setup(String appStoreUrl, MustacheTemplater templater) {
		return onRequest(request -> {
			CommDao commDao = request.getAttachment(CommUserContainer.class).getCommDao();
			request.attach(CommDao.class, commDao);

			templater.clear();
			templater.put("appStoreUrl", appStoreUrl);

			String host = request.getHeader(HOST);
			if (host == null) {
				host = request.getHostAndPort();
			}
			assert host != null : "host should not be null here";

			templater.put("url.referer", request.getHeader(REFERER));
			templater.put("forum", commDao.getAppMetadata());
		});
	}

	public static AsyncServletDecorator session(MustacheTemplater templater, String sessionCookieName) {
		return servlet ->
				request -> {
					String sessionId = request.getCookie(sessionCookieName);
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
													if (response.getCookie(sessionCookieName) != null) { // servlet itself had set the session (logout request)
														return response;
													}
													HttpCookie sessionCookie = HttpCookie.of(sessionCookieName, sessionId)
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

	@FunctionalInterface
	public interface PostRenderer {
		Object render(String threadId, Post post);
	}

	@FunctionalInterface
	public interface PrivilegePredicate {
		boolean isAllowedWithNoPrivilege(HttpRequest request);
	}
}
