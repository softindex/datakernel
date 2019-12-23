package io.datakernel.vlog.servlets;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.collection.Either;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.http.*;
import io.datakernel.http.decoder.DecodeErrors;
import io.datakernel.http.di.RequestScope;
import io.datakernel.http.di.ScopeServlet;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.vlog.container.AppUserContainer;
import io.datakernel.vlog.dao.AppDao;
import io.datakernel.vlog.handler.ProgressListener;
import io.datakernel.vlog.handler.VideoMultipartHandler;
import io.datakernel.vlog.view.VideoView;
import io.global.appstore.AppStore;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.UserData;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.HttpHeaders.HOST;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.vlog.GlobalVlogApp.SESSION_ID;
import static io.datakernel.vlog.util.HttpEntities.PAGINATION_DECODER;
import static io.datakernel.vlog.util.HttpEntities.Pagination;
import static io.datakernel.vlog.util.ViewEntityUtil.videoViewFrom;
import static io.datakernel.vlog.util.ViewEntityUtil.videoViewListFrom;
import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;
import static io.global.Utils.isGzipAccepted;
import static io.global.comm.dao.ThreadDao.ATTACHMENT_NOT_FOUND;

public final class PublicServlet {
	private static final int CHILDREN_DEPTH = 1;
	private static final int MIN_LIMIT_ITEMS_PER_PAGE = 50;
	private static final StructuredCodec<Map<String, Double>> MAP_CODEC = StructuredCodecs.ofMap(StructuredCodecs.STRING_CODEC, StructuredCodecs.DOUBLE_CODEC);
	private static String ROOT_CATEGORY = "root";

	public static AsyncServlet create(String appStoreUrl, AppStore appStore, MustacheTemplater templater, Injector injector) {
		return RoutingServlet.create()
				.map("/auth/*", AuthServlet.create(appStore, templater))
				.map("/private/*", PrivateServlet.create(templater, injector)
						.then(servlet ->
								request -> {
									UserId userId = request.getAttachment(UserId.class);
									UserData user = request.getAttachment(UserData.class);
									if (userId == null || user == null) {
										return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
									}
									return servlet.serve(request);
								})
				)
				.map(GET, "/", new ScopeServlet(injector) {
					@Provides
					@RequestScope
					Promise<HttpResponse> response(HttpRequest request) {
						VideoMultipartHandler videoMultipartHandler = request.getAttachment(VideoMultipartHandler.class);
						Either<Pagination, DecodeErrors> result = PAGINATION_DECODER.decode(request);
						if (result.isLeft()) {
							Pagination pagination = result.getLeft();
							CommDao commDao = request.getAttachment(CommDao.class);
							UserId userId = request.getAttachment(UserId.class);
							return commDao.getThreads(ROOT_CATEGORY).get()
									.then(threads -> videoViewListFrom(commDao,
											threads,
											userId,
											pagination.getPage() - 1,
											pagination.getSize(),
											videoMultipartHandler.pendingView()
									)
											.map(list -> {
												list.sort(VideoView::compareTo);
												return list;
											})
											.then(list -> templater.render("vlogViewList",
													map("list", list, "amountItems", threads.size()),
													isGzipAccepted(request))));
						}
						return Promise.of(redirect302("/?page=1&size=" + MIN_LIMIT_ITEMS_PER_PAGE));
					}
				})
				.map(GET, "/thread/:videoViewID/", new ScopeServlet(injector) {
					@Provides
					@RequestScope
					Promise<HttpResponse> response(HttpRequest request) {
						VideoMultipartHandler handler = request.getAttachment(VideoMultipartHandler.class);
						return threadViewServlet(request, templater, handler.pendingView());
					}
				})
				.map(GET, "/progress", new ScopeServlet(injector) {
					@RequestScope
					@Provides
					HttpResponse response(HttpRequest request) {
						Map<String, ProgressListener> progressListenerMap = request.getAttachment(new TypeT<Map<String, ProgressListener>>() {
						}.getType());
						if (progressListenerMap.isEmpty()) return HttpResponse.ok200();
						return HttpResponse.ok200()
								.withHeader(HttpHeaders.CONTENT_TYPE, "text/event-stream")
								.withHeader(HttpHeaders.CONNECTION, "keep-alive")
								.withBodyStream(ChannelSupplier.of(() -> Promises.delay(1000)
										.map($ -> {
											if (progressListenerMap.isEmpty()) {
												return null;
											}
											String json = JsonUtils.toJson(MAP_CODEC, progressListenerMap.entrySet()
													.stream()
													.filter(o -> o.getValue().getProgress() > 0.0)
													.collect(Collectors.toMap(Map.Entry::getKey, o -> o.getValue().getProgress())));
											return ByteBuf.wrapForReading(("data: " + json + " \n\n").getBytes());
										}))
								);
					}
				})
				.map(GET, "/thread/:videoViewID/download/:filename", downloadServlet())
				.then(session(templater))
				.then(setup(appStoreUrl, templater));
	}

	private static AsyncServlet downloadServlet() {
		return AsyncServletDecorator.create()
				.then(servlet ->
						request -> {
							String id = request.getPathParameter("videoViewID");
							return request.getAttachment(CommDao.class).getThreadDao(id)
									.then(dao -> {
										if (dao == null) {
											return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "No thread with id " + id));
										}
										request.attach(ThreadDao.class, dao);
										return servlet.serveAsync(request);
									});
						})
				.serve(request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					String filename = request.getPathParameter("filename");
					return threadDao.attachmentSize(VIDEO_VIEW_HEADER, filename)
							.thenEx((size, e) -> {
								if (e == null) {
									return HttpResponse.file(
											(offset, limit) -> threadDao.loadAttachment(VIDEO_VIEW_HEADER, filename, offset, limit),
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
				});
	}

	@NotNull
	private static Promise<HttpResponse> threadViewServlet(HttpRequest request, MustacheTemplater templater, List<String> pending) {
		String tid = request.getPathParameter("videoViewID");
		UserData userData = request.getAttachment(UserData.class);
		CommDao commDao = request.getAttachment(CommDao.class);
		UserId userId = request.getAttachment(UserId.class);
		return videoViewFrom(commDao, tid, userId, CHILDREN_DEPTH, pending)
				.then(videoView -> {
							if (videoView == null) {
								return Promise.of(redirect302("/"));
							}
							return videoView.getDeletedBy() != null && (userData == null || !userData.getRole().isPrivileged()) ?
									Promise.ofException(HttpException.ofCode(403, "Not privileged")) :
									templater.render("vlogView", map("videoView", videoView),
											isGzipAccepted(request));
						}
				);
	}

	private static AsyncServletDecorator setup(String appStoreUrl, MustacheTemplater templater) {
		return servlet ->
				request -> {
					AppUserContainer container = request.getAttachment(AppUserContainer.class);
					AppDao appDao = container.getAppDao();
					Map<String, ProgressListener> progressListenerMap = container.getProgressListenerMap();
					request.attach(new TypeT<Map<String, ProgressListener>>() {}.getType(), progressListenerMap);
					request.attach(VideoMultipartHandler.class, container.getVideoMultipartHandler());
					request.attach(AppDao.class, appDao);
					request.attach(CommDao.class, appDao.getCommDao());
					String header = request.getHeader(HOST);
					if (header == null) {
						return Promise.ofException(HttpException.ofCode(404, "cannot get host"));
					}
					templater.clear();
					templater.put("appStoreUrl", appStoreUrl);
					templater.put("app", appDao.getAppMetadata());
					return servlet.serve(request);
				};
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
					return sessionStore
							.get(sessionId)
							.then(userId -> {
								Promise<Duration> maxAge;
								if (userId != null) {
									maxAge = commDao.getUsers()
											.get(userId)
											.then(user -> {
												if (user == null) {
													return Promise.ofException(HttpException.ofCode(401, "Not authorized"));
												}
												templater.put("user", user);
												request.attach(userId);
												request.attach(user);
												return Promise.of(sessionStore.getSessionLifetimeHint());
											});
								} else {
									maxAge = Promise.of(Duration.ZERO);
								}
								return maxAge.then(age -> servlet.serveAsync(request)
										.map(response -> {
											// servlet itself had set the session (logout request)
											if (response.getCookie(SESSION_ID) != null) return response;
											HttpCookie sessionCookie = HttpCookie.of(SESSION_ID, sessionId)
													.withPath("/");
											if (age != null){
												sessionCookie.setMaxAge(age);
											}
											return response.withCookie(sessionCookie);
										}));
							});
				};
	}
}
