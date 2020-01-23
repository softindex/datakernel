package io.global.blog.http;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.collection.Either;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.ref.Ref;
import io.datakernel.http.*;
import io.datakernel.http.decoder.DecodeErrors;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.appstore.AppStore;
import io.global.blog.container.BlogUserContainer;
import io.global.blog.dao.BlogDao;
import io.global.blog.http.view.PostView;
import io.global.blog.interceptors.Preprocessor;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.http.AttachmentDataHandler;
import io.global.comm.pojo.AttachmentType;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.common.PubKey;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.common.MemSize.kilobytes;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.global.Utils.*;
import static io.global.blog.util.EntityUtil.blogViewListFrom;
import static io.global.blog.util.EntityUtil.postViewFrom;
import static io.global.blog.util.HttpUtil.*;
import static io.global.comm.dao.ThreadDao.ATTACHMENT_NOT_FOUND;
import static io.global.comm.pojo.UserRole.COMMON;
import static io.global.comm.pojo.UserRole.OWNER;
import static io.global.ot.session.AuthService.DK_APP_STORE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

public final class PublicServlet {
	private static final String SESSION_ID = "BLOG_SID";
	private static final StructuredCodec<Map<String, String>> PARAM_CODEC = StructuredCodecs.ofMap(STRING_CODEC, STRING_CODEC);
	private static final int MIN_LIMIT_ITEMS_PER_PAGE = 50;

	public static AsyncServlet create(String appStoreUrl, AppStore appStore,
			MustacheTemplater templater, Executor executor,
			Preprocessor<PostView> threadListPostViewPreprocessor,
			Preprocessor<PostView> postViewPreprocessor) {
		return RoutingServlet.create()
				.map("/auth/*", authServlet(appStore, templater))
				.map("/owner/*", ownerServlet(templater))
				.map(GET, "/", request -> {
					Either<Pagination, DecodeErrors> result = PAGINATION_DECODER.decode(request);
					if (result.isLeft()) {
						Pagination pagination = result.getLeft();
						CommDao commDao = request.getAttachment(CommDao.class);
						UserId userId = request.getAttachment(UserId.class);
						return commDao.getThreads("root").get()
								.then(threads -> blogViewListFrom(commDao, threads, userId, pagination.getPage() - 1, pagination.getSize())
										.then(threadViews -> Promise.ofBlockingCallable(executor, () -> {
											threadViews.forEach(threadView ->
													threadListPostViewPreprocessor.process(threadView.getRoot(), threadView.getId()));
											return threadViews;
										}))
										.then(threadsViews -> templater.render("threadList"
												, map("threads", threadsViews, "amountItems", threads.size()),
												isGzipAccepted(request))));
					}
					return Promise.of(redirect302("/?page=1&size=" + MIN_LIMIT_ITEMS_PER_PAGE));
				})
				.map(GET, "/profile", request -> {
					UserId userId = request.getAttachment(UserId.class);
					return userId == null ?
							Promise.of(redirectToLogin(request)) :
							templater.render("profile",
									map("shownUser", new Ref<>("user"), "userId", userId.getAuthId()),
									isGzipAccepted(request));
				})
				.map(GET, "/profile/:userId", request -> {
					UserId userId = request.getAttachment(UserId.class);
					UserData user = request.getAttachment(UserData.class);
					if (userId == null || user == null) {
						return Promise.of(redirectToLogin(request));
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					if (!user.getRole().isPrivileged()) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}
					UserId shownUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					return commDao
							.getUsers().get(shownUserId)
							.then(shownUser -> {
								if (shownUser == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								return templater.render("profile",
										map("shownUser", shownUser, "userId", shownUserId.getAuthId()),
										isGzipAccepted(request));
							});
				})
				.map(POST, "/profile/:userId", loadBody().serve(request -> {
					UserId userId = request.getAttachment(UserId.class);
					UserData user = request.getAttachment(UserData.class);
					if (userId == null || user == null) {
						return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
					}
					UserId updatingUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					if (!(userId.equals(updatingUserId) || user.getRole().isPrivileged())) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}

					Map<String, String> params = request.getPostParameters();
					String email = params.get("email");
					String username = params.get("username");
					String firstName = params.get("first_name");
					String lastName = params.get("last_name");
					return validate(email, 64, "Email", true)
							.then($ -> validate(username, 64, "Username", true))
							.then($ -> validate(firstName, 64, "FirstName"))
							.then($ -> validate(lastName, 64, "LastName"))
							.then($ -> {
								CommDao commDao = request.getAttachment(CommDao.class);
								return commDao.getUsers().get(updatingUserId).then(oldData -> oldData == null ?
										Promise.ofException(HttpException.ofCode(400, "No such user")) :
										commDao.getUsers().put(updatingUserId, new UserData(oldData.getRole(), email, username, firstName, lastName))
												.map($1 -> redirectToReferer(request, "/")));
							});
				}))
				.map("/:threadID/*", RoutingServlet.create()
						.map(GET, "/", postViewServlet(templater, executor, postViewPreprocessor))
						.map(GET, "/:postID/", postViewServlet(templater, executor, postViewPreprocessor))
						.map("/:postID/*", postOperations())
						.then(attachThreadDao()))
				.then(session(templater))
				.then(setup(appStoreUrl, templater));
	}

	private static AsyncServlet ownerServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/new", request -> templater.render("newThread", map("creatingNewThread", true), isGzipAccepted(request)))
				.map(POST, "/new", request -> {
					UserId userId = request.getAttachment(UserId.class);
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.generateThreadId()
							.then(id -> commDao.getThreads("root").put(id, ThreadMetadata.of("<unnamed>", 0))
									.map($ -> id))
							.then(tid -> commDao.getThreadDao(tid)
									.then(threadDao -> {
										assert threadDao != null : "No thread dao just after creating the thread";
										Map<String, AttachmentType> attachmentMap = new HashMap<>();
										Map<String, String> paramsMap = new HashMap<>();
										return request.handleMultipart(AttachmentDataHandler.create(threadDao, "root", emptySet(), paramsMap, attachmentMap, true))
												.then($ -> validate(paramsMap.get("title"), 120, "Title", true))
												.then($ -> validate(paramsMap.get("content"), 65256, "Content"))
												.then($ -> commDao.getThreads("root").put(tid, ThreadMetadata.of(paramsMap.get("title"), 0))
														.then($2 -> threadDao.addRootPost(userId, paramsMap.get("content"), attachmentMap))
														.map($2 -> redirect302("/" + tid)))
												.thenEx(revertIfException(() -> threadDao.deleteAttachments("root", attachmentMap.keySet())))
												.thenEx(revertIfException(() -> commDao.getThreads("root").remove(tid)));
									}));
				})
				.map(POST, "/blog", loadBody(kilobytes(256))
						.serve(request -> {
							try {
								Map<String, String> params = JsonUtils.fromJson(PARAM_CODEC, request.getBody().getString(UTF_8));
								String description = params.get("content");
								String title = params.get("title");
								BlogDao blogDao = request.getAttachment(BlogDao.class);
								return validate(description, 1024, "Description")
										.then($ -> validate(title, 32, "Name"))
										.then($ -> (title != null ? blogDao.setBlogName(title) : Promise.complete())
												.then($1 -> description != null ? blogDao.setBlogDescription(description) : Promise.complete())
												.map($1 -> HttpResponse.ok200()));
							} catch (ParseException e) {
								return Promise.ofException(new ParseException("Illegal arguments", e));
							}
						}))
				.map("/:threadID/*", RoutingServlet.create()
						.map(POST, "/edit/", loadBody(kilobytes(256))
								.serve(request -> {
									try {
										Map<String, String> params = JsonUtils.fromJson(PARAM_CODEC, request.getBody().getString(UTF_8));
										String title = params.get("title");
										return validate(title, 120, "Title", true)
												.then($ -> request.getAttachment(CommDao.class)
														.getThreads("root").put(request.getPathParameter("threadID"), ThreadMetadata.of(title, 0))
														.map($1 -> HttpResponse.ok200()));
									} catch (ParseException e) {
										return Promise.ofException(new ParseException("Illegal arguments", e));
									}
								}))
						.map(POST, "/root/edit/", request -> {
							String threadID = request.getPathParameter("threadID");
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);

							Map<String, AttachmentType> attachmentMap = new HashMap<>();
							Map<String, String> paramsMap = new HashMap<>();

							return threadDao.listAttachments("root")
									.then(list -> request.handleMultipart(AttachmentDataHandler.create(threadDao, "root", list, paramsMap, attachmentMap, true)))
									.then($ -> {
										String deleteAttachments = paramsMap.get("deleteAttachments");
										String content = paramsMap.get("content");
										Set<String> deleteAttachmentsSet = deleteAttachments.isEmpty() ?
												emptySet() :
												Stream.of(deleteAttachments.split(","))
														.filter(val -> !val.isEmpty())
														.collect(Collectors.toSet());
										return validate(content, 65256, "Content", true)
												.then($1 -> threadDao.updatePost("root", content, attachmentMap, deleteAttachmentsSet)
														.then($2 -> threadDao.deleteAttachments("root", deleteAttachmentsSet))
														.map($2 -> redirect302("/" + threadID)));
									})
									.thenEx(revertIfException(() -> threadDao.deleteAttachments("root", attachmentMap.keySet())));
						})
						.then(attachThreadDao())
				)
				.then(servlet ->
						request -> {
							UserId userId = request.getAttachment(UserId.class);
							UserData user = request.getAttachment(UserData.class);
							if (userId == null || user == null) {
								return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
							}
							if (!user.getRole().isPrivileged()) {
								return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
							}
							return servlet.serve(request);
						});
	}

	private static AsyncServlet authServlet(AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/login", request -> {
					String origin = request.getQueryParameter("origin");
					origin = origin != null ? origin : request.getHeader(REFERER);
					origin = origin != null ? origin : "/";
					return request.getAttachment(UserId.class) != null ?
							Promise.of(redirect302(origin)) :
							templater.render("login", map("loginScreen", true, "origin", origin), isGzipAccepted(request));
				})
				.map(POST, "/logout", request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(HttpResponse.ok200());
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> HttpResponse.redirect302("/")
									.withCookie(HttpCookie.of(SESSION_ID)
											.withPath("/")
											.withMaxAge(Duration.ZERO)));
				})
				.map(GET, "/authorize", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStore.exchangeAuthToken(token).then(profile -> {
						CommDao commDao = request.getAttachment(CommDao.class);
						PubKey containerPubKey = commDao.getKeys().getPubKey();
						PubKey pubKey = profile.getPubKey();
						UserId userId = new UserId(DK_APP_STORE, pubKey.asString());
						String sessionId = generateString(32);
						return commDao.getUsers().get(userId)
								.then(existing -> {
									if (existing != null) {
										return Promise.complete();
									}
									return commDao.getUsers().put(userId,
											new UserData(
													containerPubKey.equals(pubKey) ? OWNER : COMMON,
													profile.getEmail(), profile.getUsername(),
													profile.getFirstName(), profile.getLastName(), null));
								})
								.then($ -> {
									SessionStore<UserId> sessionStore = commDao.getSessionStore();
									return sessionStore.save(sessionId, userId).map($2 -> {
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
				});
	}

	private static HttpResponse redirectToLogin(HttpRequest request) {
		return redirect302("/login?origin=" + request.getPath());
	}

	private static HttpResponse postOpRedirect(HttpRequest request) {
		String tid = request.getPathParameter("threadID");
		String pid = request.getPathParameter("postID");
		return redirectToReferer(request, "/" + tid + "/" + pid);
	}

	@NotNull
	private static AsyncServlet postOperations() {
		return RoutingServlet.create()
				.map(POST, "/", loadBody(kilobytes(512))
						.serve(request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);

							UserId user = request.getAttachment(UserId.class);
							String parentId = request.getPathParameter("postID");
							String content = request.getPostParameter("content");
							return validate(content, 1024, "Content", true)
									.then($ -> threadDao.generatePostId())
									.then(pid -> threadDao.addPost(user, parentId, pid, content, Collections.emptyMap()).toVoid()
											.map($1 -> postOpRedirect(request)));
						}))
				.map(POST, "/delete", request ->
						request.getAttachment(ThreadDao.class)
								.removePost(request.getAttachment(UserId.class), request.getPathParameter("postID"))
								.map($ -> postOpRedirect(request)))
				.map(POST, "/restore", request ->
						request.getAttachment(ThreadDao.class).restorePost(request.getPathParameter("postID"))
								.map($ -> postOpRedirect(request)))
				.map(POST, "/edit", loadBody(kilobytes(256))
						.serve(request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							String pid = request.getPathParameter("postID");
							String content = request.getPostParameter("content");
							return validate(content, 1024, "Content", true)
									.then($ -> threadDao.updatePost(pid, content, Collections.emptyMap(), emptySet())
											.map($2 -> postOpRedirect(request)));
						}))
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
											request.getHeader(HttpHeaders.RANGE)
									);
								} else if (e == ATTACHMENT_NOT_FOUND) {
									return Promise.of(HttpResponse.notFound404());
								} else {
									return Promise.<HttpResponse>ofException(e);
								}
							});
				}))
				.then(servlet ->
						request -> request.getMethod() == POST && request.getAttachment(UserId.class) == null ?
								Promise.ofException(HttpException.ofCode(401, "Not authorized")) :
								servlet.serve(request));
	}

	@NotNull
	private static AsyncServlet postViewServlet(MustacheTemplater templater, Executor executor, Preprocessor<PostView> postViewPreprocessor) {
		return request -> {
			String tid = request.getPathParameter("threadID");
			String pid = request.getPathParameters().get("postID");
			CommDao commDao = request.getAttachment(CommDao.class);
			if ("root".equals(pid)) {
				return Promise.of(redirect302("../"));
			}
			UserData userData = request.getAttachment(UserData.class);
			UserId userId = request.getAttachment(UserId.class);
			ThreadDao threadDao = request.getAttachment(ThreadDao.class);

			Promise<PostView> postViewPromise = threadDao.getPost(pid != null ? pid : "root")
					.then(post -> postViewFrom(commDao, post, userId, 2, null))
					.then(postView -> Promise.ofBlockingCallable(executor,
							() -> pid == null ? postViewPreprocessor.process(postView, tid) : postView));
			return postViewPromise.then(postView ->
					postView.getDeletedBy() != null && (userData == null || !userData.getRole().isPrivileged()) ?
							Promise.ofException(HttpException.ofCode(403, "Not privileged")) :
							templater.render(pid != null ? "subthread" : "thread", map(
									"threadId", tid,
									"thread", threadDao.getThreadMetadata(),
									"post", postView),
									isGzipAccepted(request)));
		};
	}

	private static AsyncServletDecorator setup(String appStoreUrl, MustacheTemplater templater) {
		return onRequest(request -> {
			BlogDao blogDao = request.getAttachment(BlogUserContainer.class).getBlogDao();
			request.attach(BlogDao.class, blogDao);
			request.attach(CommDao.class, blogDao.getCommDao());
			PubKey pubKey = blogDao.getKeys().getPubKey();
			request.attach(PubKey.class, pubKey);
			templater.clear();
			templater.put("appStoreUrl", appStoreUrl);
			templater.put("blog", blogDao.getBlogMetadata());
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
					return sessionStore.get(sessionId).then(userId -> {
						Promise<Duration> maxAge = userId != null ?
								commDao.getUsers().get(userId).map(user -> {
									templater.put("user", user);
									request.attach(userId);
									request.attach(user);
									return sessionStore.getSessionLifetimeHint();
								}) :
								Promise.of(Duration.ZERO);
						return maxAge.then(age -> servlet.serve(request)
								.get()
								.map(response -> {
									HttpCookie sessionCookie = HttpCookie.of(SESSION_ID, sessionId)
											.withPath("/");
									if (age != null) {
										sessionCookie.setMaxAge(age);
									}
									return response.getCookie(SESSION_ID) != null ? // servlet itself had set the session (logout request)
											response :
											response.withCookie(sessionCookie);
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
