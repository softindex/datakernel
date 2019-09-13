package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.util.ref.Ref;
import io.global.appstore.AppStore;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.*;
import io.global.common.PubKey;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.http.view.PostView;
import io.global.forum.http.view.ThreadView;
import io.global.forum.util.MustacheTemplater;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpHeaders.HOST;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.Utils.generateString;
import static io.global.comm.pojo.AuthService.DK_APP_STORE;
import static io.global.forum.util.Utils.redirectToReferer;
import static io.global.forum.util.Utils.revertIfException;

public final class PublicServlet {
	private static final String SESSION_ID = "FORUM_SID";
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";

	public static AsyncServlet create(String appStoreUrl, AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					CommDao commDao = request.getAttachment(CommDao.class);
					UserId userId = request.getAttachment(UserId.class);
					return commDao.getThreads()
							.then(threads -> ThreadView.from(commDao, threads, userId))
							.then(threads -> templater.render("threadList", map("threads", threads)));
				})
				.map(GET, "/login", request -> {

					String origin = request.getQueryParameter("origin");
					if (origin == null) {
						origin = request.getHeader(REFERER);
					}
					if (origin == null) {
						origin = "/" + request.getAttachment(PubKey.class).asString();
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
														String pk = containerPubKey.asString();
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
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> {
								String pk = commDao.getKeys().getPubKey().asString();
								return redirectToReferer(request, "/" + pk)
										.withCookie(HttpCookie.of(SESSION_ID, "<unset>")
												.withPath("/" + pk)
												.withMaxAge(Duration.ZERO));
							});
				})
				.map(GET, "/new", request -> {
					if (request.getAttachment(UserId.class) == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("newThread", map("creatingNewThread", true));
				})
				.map(POST, "/new", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					CommDao commDao = request.getAttachment(CommDao.class);

					return commDao.createThread(new ThreadMetadata("<unnamed>"))
							.then(tid -> {
								ThreadDao threadDao = commDao.getThreadDao(tid);
								assert threadDao != null : "No thread dao just after creating the thread";

								Map<String, Attachment> attachmentMap = new HashMap<>();
								Map<String, String> paramsMap = new HashMap<>();

								return request.handleMultipart(AttachmentDataHandler.create(threadDao, paramsMap, attachmentMap))
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
											String pk = commDao.getKeys().getPubKey().asString();

											return commDao.updateThread(tid, new ThreadMetadata(title))
													.then($2 -> threadDao.addRootPost(userId, content, attachmentMap))
													.map($2 -> redirect302("/" + pk + "/" + tid));
										})
										.thenEx(revertIfException(() -> threadDao.deleteAttachments(attachmentMap.keySet())))
										.thenEx(revertIfException(() -> commDao.removeThread(tid)));
							});
				})
				.map(GET, "/profile", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.of(redirectToLogin(request));
					}
					return templater.render("profile", map("shownUser", new Ref<>("user"), "userId", userId.getAuthId()));
				})
				.map(GET, "/profile/:userId", request -> {
					UserId userId = request.getAttachment(UserId.class);
					if (userId == null) {
						return Promise.of(redirectToLogin(request));
					}
					return request.getAttachment(CommDao.class)
							.getUser(userId)
							.then(user -> templater.render("profile", map("shownUser", user)));
				})
				.map(POST, "/profile", request -> {
					UserId userId = request.getAttachment(UserId.class);
					UserData oldData = request.getAttachment(UserData.class);
					if (userId == null || oldData == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
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
					return commDao.updateUser(userId, new UserData(oldData.getRole(), email, username, firstName, lastName))
							.map($ -> redirectToReferer(request, "/" + commDao.getKeys().getPubKey().asString() + "/profile"));
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
		return redirect302("/" + request.getAttachment(PubKey.class).asString() + "/login?origin=" + request.getPath());
	}

	private static HttpResponse postOpRedirect(HttpRequest request) {
		String tid = request.getPathParameter("threadID");
		String pid = request.getPathParameter("postID");
		ForumDao forumDao = request.getAttachment(ForumDao.class);
		return redirectToReferer(request, "/" + forumDao.getKeys().getPubKey().asString() + "/" + tid + "/" + pid);
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
								if ((content == null || content.matches(WHITESPACE)) && attachmentMap.isEmpty()) {
									return Promise.ofException(new ParseException(PublicServlet.class, "'content' POST parameter is required"));
								}
								return threadDao.addPost(user, pid, content, attachmentMap).toVoid();
							})
							.thenEx(revertIfException(() -> threadDao.deleteAttachments(attachmentMap.keySet())))
							.map($ -> postOpRedirect(request));
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
					if (request.getMethod() == POST && request.getAttachment(UserId.class) == null) {
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
			CommDao commDao = request.getAttachment(CommDao.class);
			if ("root".equals(pid)) {
				return Promise.of(redirect302("../"));
			}
			UserId userId = request.getAttachment(UserId.class);
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);
			return templater.render(pid != null ? "subthread" : "thread", map(
					"threadId", tid,
					"thread", threadDao.getThreadMetadata(),
					"post", threadDao.getPost(pid != null ? pid : "root").then(post -> PostView.from(commDao, post, userId, null, true))
			));
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
			templater.put("pubKey", pubKey.asString());
			templater.put("url", request.toString());
			templater.put("url.host", request.getHeader(HOST));
			templater.put("forum", forumDao.getForumMetadata());
		});
	}

	private static AsyncServletDecorator session(MustacheTemplater templater) {
		return servlet ->
				request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return servlet.serve(request);
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					SessionStore<UserId> sessionStore = commDao.getSessionStore();
					return sessionStore.get(sessionId)
							.then(userId -> {
								Promise<Duration> maxAge;
								if (userId != null) {
									maxAge = commDao.getUser(userId)
											.map(user -> {
												templater.put("user", user);
												request.attach(userId);
												request.attach(user);
												return sessionStore.getSessionLifetime();
											});
								} else {
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
																	.withPath("/" + commDao.getKeys().getPubKey().asString()));
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
