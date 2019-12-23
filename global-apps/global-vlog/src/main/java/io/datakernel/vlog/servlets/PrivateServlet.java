package io.datakernel.vlog.servlets;

import io.datakernel.common.MemSize;
import io.datakernel.common.collection.Either;
import io.datakernel.common.ref.Ref;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.decoder.DecodeErrors;
import io.datakernel.http.di.RequestScope;
import io.datakernel.http.di.ScopeServlet;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.vlog.handler.*;
import io.datakernel.vlog.util.HttpEntities.Comment;
import io.datakernel.vlog.util.HttpEntities.ProfileMetadata;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.AttachmentType;
import io.global.comm.pojo.Rating;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.NotNull;

import java.awt.*;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.datakernel.common.MemSize.kilobytes;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.http.MultipartParser.MultipartDataHandler;
import static io.datakernel.vlog.util.HttpEntities.COMMENT_DECODER;
import static io.datakernel.vlog.util.HttpEntities.PROFILE_DECODER;
import static io.datakernel.vlog.util.HttpUtil.*;
import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;
import static io.global.Utils.*;
import static io.global.ot.session.AuthService.DK_APP_STORE;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public final class PrivateServlet {
	public static AsyncServlet create(MustacheTemplater templater, Injector injector) {
		return RoutingServlet.create()
				.map("/new/", privilegedDecorator()
						.serve(newServlet(templater, injector)))
				.map("/profile/*", profileServlet(templater))
				.map("/thread/:videoViewID/*", threadServlet(injector));
	}

	private static AsyncServlet newServlet(MustacheTemplater templater, Injector injector) {
		Module paramsModule = Module.create()
				.bind(new Key<Map<String, String>>() {})
					.in(RequestScope.class)
					.toInstance(new HashMap<>())
				.bind(new Key<Map<String, AttachmentType>>() {})
					.in(RequestScope.class)
					.toInstance(new HashMap<>())
				.bind(new Key<Promise<String>>() {})
					.in(RequestScope.class)
					.to(request -> {
						CommDao commDao = request.getAttachment(CommDao.class);
						return commDao.generateThreadId()
								.then(id -> commDao.getThreads("root")
										.put(id, ThreadMetadata.of("<unnamed>", 0))
										.then($ -> commDao.getThreadDao(id)
												.map(dao -> {
													request.attach(ThreadDao.class, dao);
													return id;
												})
										)
								);
					}, HttpRequest.class);
		return RoutingServlet.create()
				.map(GET, "/", request -> templater.render("newVideo", isGzipAccepted(request)))
				.map(POST, "/", new ScopeServlet(injector, paramsModule) {
					@Provides
					Map<String, Dimension> resolutions() {
						return map("1080", new Dimension(1920, 1080),
								"720", new Dimension(1200, 720),
								"320", new Dimension(352, 240));
					}

					@Provides
					@RequestScope
					Promise<ProgressListener> progressListener(Promise<String> idPromise, HttpRequest request, Map<String, Dimension> resolutions, ExecutorService executorService) {
						return idPromise.map(id -> {
							Map<String, ProgressListener> progressListenerMap = request.getAttachment(new TypeT<Map<String, ProgressListener>>() {
							}.getType());
							int listeners = resolutions.size();
							ProgressListenerImpl progressListener = new ProgressListenerImpl(listeners + 1, listeners, (result, e) -> progressListenerMap.remove(id));
							progressListenerMap.put(id, progressListener);
							return progressListener;
						});
					}

					@Provides
					@RequestScope
					Promise<List<VideoHandler>> videoHandlers(HttpRequest request, Promise<String> idPromise, Map<String, String> params,
															  Eventloop eventloop, ExecutorService executor, Promise<ProgressListener> progressListenerPromise,
															  Map<String, Dimension> resolutions, @Named("cached") Path cachedPath) {
						return progressListenerPromise.then(progressListener ->
								idPromise.map(id -> {
									ThreadDao threadDao = request.getAttachment(ThreadDao.class);
									return resolutions.entrySet()
											.stream()
											.map(entry -> new ThreadDaoVideoHandler(
													entry.getKey(),
													threadDao,
													executor,
													eventloop,
													entry.getValue(),
													cachedPath,
													progressListener)
											)
											.collect(Collectors.toList());
								})
						);
					}

					@SuppressWarnings("ConstantConditions")
					@Provides
					@RequestScope
					Promise<VideoPosterHandler> videoPosterHandler(HttpRequest request, Promise<String> promiseId, Eventloop eventloop,
																   ExecutorService executor, Promise<ProgressListener> progressListenerPromise) {
						CommDao commDao = request.getAttachment(CommDao.class);
						return progressListenerPromise
								.then(progressListener ->
										promiseId.then(tid -> commDao.getThreadDao(tid)
												.map(threadDao -> (VideoPosterHandler) new JavaCvVideoPosterHandler(
														threadDao,
														executor,
														eventloop,
														new Dimension(150, 150),
														progressListener)
												)
										)
								);
					}

					@Provides
					@RequestScope
					ExecutorService executor(Map<String, Dimension> formats) {
						return Executors.newFixedThreadPool(formats.size());
					}

					@Provides
					@RequestScope
					Promise<MultipartDataHandler> videoMultipartHandler(HttpRequest request, Map<String, AttachmentType> attachmentMap, Map<String, String> paramsMap,
																		Promise<List<VideoHandler>> videoHandlers, Promise<String> promiseId,
																		Promise<VideoPosterHandler> videoPosterHandler, ExecutorService executor) {
						CommDao commDao = request.getAttachment(CommDao.class);
						VideoMultipartHandler videoMultipartHandler = request.getAttachment(VideoMultipartHandler.class);
						return promiseId.then(tid -> videoHandlers
								.then(handlers -> commDao.getThreadDao(tid)
										.then(threadDao -> videoPosterHandler.map(posterHandler -> {
													Predicate<Map<String, String>> condition = map -> {
														if (validate(map.get("title"), 120, true) && validate(map.get("content"), 65256)) {
															paramsMap.putAll(map);
															return true;
														}
														return false;
													};
													return videoMultipartHandler.create(tid, threadDao, attachmentMap, handlers, posterHandler, commDao, condition, executor);
												})
										)
								)
						);
					}

					@Provides
					@RequestScope
					Promise<HttpResponse> response(Promise<Void> task) {
						return task.mapEx(($, e) -> redirect302("/"));
					}

					@RequestScope
					@Provides
					Promise<Void> mainTask(HttpRequest request, Promise<String> idPromise, Map<String, AttachmentType> attachmentMap,
										   Promise<MultipartDataHandler> multipartDataHandlerPromise, Map<String, String> paramsMap) {
						Map<String, ProgressListener> progressListenerMap = request.getAttachment(new TypeT<Map<String, ProgressListener>>() {
						}.getType());
						return Promises.toTuple(idPromise, multipartDataHandlerPromise)
								.then(tuple -> {
									UserId userId = request.getAttachment(UserId.class);
									CommDao commDao = request.getAttachment(CommDao.class);
									ThreadDao threadDao = request.getAttachment(ThreadDao.class);
									String tid = tuple.getValue1();
									MultipartDataHandler multipartDataHandler = tuple.getValue2();
									return request.handleMultipart(multipartDataHandler)
											.then($ -> commDao.getThreads("root")
													.put(tid, ThreadMetadata.of(paramsMap.get("title"), 0))
											)
											.then($ -> threadDao.addRootPost(userId, paramsMap.get("content"), VIDEO_VIEW_HEADER, attachmentMap))
											.thenEx(revertIfException(() -> {
												progressListenerMap.remove(tuple.getValue1());
												return commDao.getThreads("root")
														.remove(tid);
											}));
								});
					}
				});
	}

	private static AsyncServlet profileServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", request -> {
					UserId userId = request.getAttachment(UserId.class);
					return templater.render("profile",
							map("shownUser", new Ref<>("user"), "userId", userId.getAuthId()), isGzipAccepted(request));
				})
				.map(GET, "/:userId", request -> {
					UserData user = request.getAttachment(UserData.class);
					CommDao commDao = request.getAttachment(CommDao.class);
					if (!user.getRole().isPrivileged()) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}
					UserId shownUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
					return commDao
							.getUsers()
							.get(shownUserId)
							.then(shownUser -> {
								if (shownUser == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(400, "No such user"));
								}
								return templater.render("profile",
										map("shownUser", shownUser, "userId", shownUserId.getAuthId()),
										isGzipAccepted(request));
							});
				})
				.map(POST, "/:userId/", loadBody(512)
						.serve(request -> {
							UserId userId = request.getAttachment(UserId.class);
							UserData user = request.getAttachment(UserData.class);
							UserId updatingUserId = new UserId(DK_APP_STORE, request.getPathParameter("userId"));
							if (!(userId.equals(updatingUserId) || user.getRole().isPrivileged())) {
								return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
							}
							Either<ProfileMetadata, DecodeErrors> result = PROFILE_DECODER.decode(request);
							if (result.isLeft()) {
								ProfileMetadata profileMetadata = result.getLeft();
								CommDao commDao = request.getAttachment(CommDao.class);
								return commDao.getUsers()
										.get(updatingUserId)
										.then(oldData -> oldData == null ?
												Promise.ofException(HttpException.ofCode(400, "No such user")) :
												commDao.getUsers()
														.put(updatingUserId, profileMetadata.toUserData(oldData.getRole())))
										.map($1 -> redirectToReferer(request, "/"));
							}
							return Promise.ofException(decodeErrorsToHttpException(result.getRight()));
						})
				);
	}

	private static AsyncServlet threadServlet(Injector injector) {
		return RoutingServlet.create()
				.map(POST, "/edit/", privilegedDecorator()
						.serve(new ScopeServlet(injector, Module.create()
								.bind(new Key<Map<String, String>>() {})
									.in(RequestScope.class)
									.toInstance(new HashMap<>())) {
							@Provides
							@RequestScope
							ExecutorService executor() {
								return Executors.newSingleThreadExecutor();
							}

							@Provides
							@RequestScope
							MultipartDataHandler multipartDataHandler(HttpRequest request, ExecutorService executor, Map<String, String> params) {
								ThreadDao threadDao = request.getAttachment(ThreadDao.class);
								return PosterMultipartHandler.create(executor, threadDao, params, MemSize.megabytes(52), new Dimension(300, 300),
										paramsMap -> {
											if (validate(paramsMap.get("title"), 32, true) &&
													validate(paramsMap.get("content"), 1024, true)) {
												params.putAll(paramsMap);
												return true;
											}
											return false;
										});
							}

							@RequestScope
							@Provides
							Promise<HttpResponse> response(HttpRequest request, MultipartDataHandler multipartDataHandler, Map<String, String> params, ExecutorService executorService) {
								String videoViewID = request.getPathParameter("videoViewID");
								ThreadDao threadDao = request.getAttachment(ThreadDao.class);
								CommDao commDao = request.getAttachment(CommDao.class);
								return request.handleMultipart(multipartDataHandler)
										.then($ -> commDao.getThreads("root")
												.put(videoViewID, ThreadMetadata.of(params.get("title"), 0))
												.then($1 -> threadDao.updatePost(VIDEO_VIEW_HEADER, params.get("content"), emptyMap(), emptySet()))
												.map($1 -> redirect302("/thread/" + videoViewID))
										)
										.whenComplete(executorService::shutdown);
							}
						}))
				.map("/:commentID/*", commentOperations())
				.then(attachThreadDao());
	}

	@NotNull
	private static AsyncServlet commentOperations() {
		return RoutingServlet.create()
				.map(POST, "/", loadBody(kilobytes(512))
						.serve(request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							UserId user = request.getAttachment(UserId.class);
							Either<Comment, DecodeErrors> result = COMMENT_DECODER.decode(request);
							if (result.isLeft()) {
								Comment comment = result.getLeft();
								return threadDao.generatePostId()
										.then(pid -> threadDao.addPost(user, comment.getParentId(), pid, comment.getContent(), emptyMap()))
										.map($ -> postOpRedirect(request));
							}
							return Promise.ofException(decodeErrorsToHttpException(result.getRight()));
						}))
				.map(POST, "/rate/:rating", loadBody(kilobytes(512))
						.serve(request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							UserId userId = request.getAttachment(UserId.class);
							Rating rating = Rating.fromString(request.getPathParameter("rating"));
							return threadDao.getPost(VIDEO_VIEW_HEADER)
									.then(post -> {
										if (post.getRatings().get(rating).contains(userId)) {
											return threadDao.updateRating(userId, VIDEO_VIEW_HEADER, null);
										} else {
											return threadDao.updateRating(userId, VIDEO_VIEW_HEADER, rating);
										}
									})
									.map($ -> redirectToReferer(request, "/"));
						}))
				.map(POST, "/delete", request -> {
					UserData user = request.getAttachment(UserData.class);
					UserId userId = request.getAttachment(UserId.class);
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					return threadDao.getPost(request.getPathParameter("commentID"))
							.then(post -> {
								if (!user.getRole().isPrivileged() || !userId.equals(post.getAuthor())) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(403, "Not privileged"));
								}
								String commentID = request.getPathParameter("commentID");
								if (commentID.equals(VIDEO_VIEW_HEADER)) {
									String videoViewID = request.getPathParameter("videoViewID");
									CommDao commDao = request.getAttachment(CommDao.class);
									return threadDao.listAttachments(VIDEO_VIEW_HEADER)
											.then(list -> threadDao.deleteAttachments(VIDEO_VIEW_HEADER, list))
											.then($ -> commDao.getThreads("root").remove(videoViewID))
											.map($ -> redirect302("/"));
								}
								return request.getAttachment(ThreadDao.class)
										.removePost(request.getAttachment(UserId.class), commentID)
										.map($ -> postOpRedirect(request));
							});
				})
				.map(POST, "/restore", request -> {
					UserData user = request.getAttachment(UserData.class);
					UserId userId = request.getAttachment(UserId.class);
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					String commentID = request.getPathParameter("commentID");
					return threadDao.getPost(commentID)
							.then(post -> {
								if (!user.getRole().isPrivileged() || !userId.equals(post.getAuthor())) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(403, "Not privileged"));
								}
								return threadDao.restorePost(commentID);
							})
							.map($ -> postOpRedirect(request));
				})
				.map(POST, "/edit", loadBody(kilobytes(256))
						.serve(request -> {
							UserData user = request.getAttachment(UserData.class);
							UserId userId = request.getAttachment(UserId.class);
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							return threadDao.getPost(request.getPathParameter("commentID"))
									.then(post -> {
										if (!user.getRole().isPrivileged() || !userId.equals(post.getAuthor())) {
											return Promise.<HttpResponse>ofException(HttpException.ofCode(403, "Not privileged"));
										}
										Either<Comment, DecodeErrors> result = COMMENT_DECODER.decode(request);
										if (result.isLeft()) {
											Comment comment = result.getLeft();
											return threadDao.updatePost(comment.getParentId(), comment.getContent(), emptyMap(), emptySet())
													.map($ -> postOpRedirect(request));
										}
										return Promise.ofException(decodeErrorsToHttpException(result.getRight()));
									});
						})
				);
	}

	private static AsyncServletDecorator privilegedDecorator() {
		return servlet ->
				request -> {
					UserData user = request.getAttachment(UserData.class);
					if (!user.getRole().isPrivileged()) {
						return Promise.ofException(HttpException.ofCode(403, "Not privileged"));
					}
					return servlet.serve(request);
				};
	}

	private static AsyncServletDecorator attachThreadDao() {
		return servlet ->
				request -> {
					String id = request.getPathParameter("videoViewID");
					return request.getAttachment(CommDao.class)
							.getThreadDao(id)
							.then(dao -> {
								if (dao == null) {
									return Promise.ofException(HttpException.ofCode(404, "No thread with id " + id));
								}
								request.attach(ThreadDao.class, dao);
								return servlet.serveAsync(request);
							});
				};
	}
}
