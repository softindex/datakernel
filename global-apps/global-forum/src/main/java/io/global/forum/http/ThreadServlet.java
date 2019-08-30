package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.datakernel.http.RoutingServlet;
import io.global.forum.dao.ThreadDao;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.util.Utils.nullToEmpty;
import static io.global.forum.Utils.*;
import static io.global.forum.dao.ThreadDao.POST_NOT_FOUND;
import static java.util.stream.Collectors.toSet;

public final class ThreadServlet {
	public static RoutingServlet create() {
		return RoutingServlet.create()
				// API
				.map(GET, "/", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					return threadDao.listPosts()
							.map(posts -> HttpResponse.ok200()
									.withJson(POSTS_ENCODER_ROOT, posts));
				})
				.map(POST, "/", request -> {
					ThreadDao threadDao = request.getAttachment(ThreadDao.class);
					UserId author = request.getAttachment(UserId.class);

					Long parentId;
					try {
						String parentIdString = request.getQueryParameter("parentId");
						if (parentIdString == null) {
							throw new ParseException();
						}
						parentId = Long.parseLong(parentIdString);
					} catch (NumberFormatException | ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, "'parentId' numerical query parameter is required"));
					}

					Map<String, Attachment> attachmentMap = new HashMap<>();
					Map<String, String> paramsMap = new HashMap<>();

					MultipartDataHandler handler = AttachmentDataHandler.create(threadDao, paramsMap, attachmentMap);
					return request.handleMultipart(handler)
							.then($ -> {
								String content = paramsMap.get("content");
								if (content == null) {
									return Promise.ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
								}
								return threadDao.addPost(author, parentId, content, attachmentMap).toVoid();
							})
							.thenEx(revertIfException(threadDao, attachmentMap))
							.map($ -> HttpResponse.ok201());
				})
				.map("/:postId/*", RoutingServlet.create()
						.map(GET, "/", request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							Long postId = request.getAttachment("postId");
							return threadDao.listPosts()
									.then(posts -> {
										Post post = posts.get(postId);
										if (post == null) {
											return Promise.<HttpResponse>ofException(POST_NOT_FOUND);
										}
										return Promise.of(HttpResponse.ok200()
												.withJson(POST_ENCODER, post));
									});
						})
						.map(GET, "/tree", request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							Long postId = request.getAttachment("postId");
							return threadDao.listPosts()
									.then(posts -> {
										Post post = posts.get(postId);
										if (post == null) {
											return Promise.<HttpResponse>ofException(POST_NOT_FOUND);
										}
										return Promise.of(HttpResponse.ok200()
												.withJson(postsEncoder(postId), posts));
									});
						})
						.map(PUT, "/", request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							Long postId = request.getAttachment("postId");

							Map<String, Attachment> attachmentMap = new HashMap<>();
							Map<String, String> paramsMap = new HashMap<>();

							MultipartDataHandler handler = AttachmentDataHandler.create(threadDao, paramsMap, attachmentMap);
							return request.handleMultipart(handler)
									.then($ -> {
										String content = paramsMap.get("content");
										String removeString = nullToEmpty(paramsMap.get("removeAttachments"));
										Set<String> toBeRemoved = Arrays.stream(removeString.split(","))
												.map(String::trim)
												.collect(toSet());

										return threadDao.updatePost(postId, content, attachmentMap, toBeRemoved);
									})
									.thenEx(revertIfException(threadDao, attachmentMap))
									.map($ -> HttpResponse.ok201());
						})
						.map(DELETE, "/", request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							UserId userId = request.getAttachment(UserId.class);
							Long postId = request.getAttachment("postId");
							return threadDao.removePost(userId, postId)
									.map($ -> HttpResponse.ok200());
						})
						.map(POST, "/rating", request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							UserId userId = request.getAttachment(UserId.class);
							Long postId = request.getAttachment("postId");

							String setRating = request.getPostParameter("setRating");
							if (setRating == null) {
								return Promise.ofException(HttpException.ofCode(400, "'setRating' POST parameter is required"));
							}
							return Promise.complete()
									.then($ -> {
										switch (setRating) {
											case "like":
												return threadDao.like(userId, postId);
											case "dislike":
												return threadDao.dislike(userId, postId);
											case "none":
												return threadDao.removeLikeOrDislike(userId, postId);
											default:
												return Promise.ofException(HttpException.ofCode(400,
														"Only 'like', 'dislike' and 'none' values are accepted"));
										}
									})
									.map($ -> HttpResponse.ok200());
						})
						.map(GET, "/:globalFsId", request -> {
							ThreadDao threadDao = request.getAttachment(ThreadDao.class);
							Long postId = request.getAttachment("postId");
							String globalFsId = request.getPathParameter("globalFsId");
							if (globalFsId.isEmpty()) {
								return Promise.ofException(HttpException.notFound404());
							}

							return threadDao.getAttachment(postId, globalFsId)
									.then(attachment -> threadDao.attachmentSize(globalFsId)
											.then(size -> {
												try {
													return Promise.of(HttpResponse.file(
															(offset, limit) ->
																	threadDao.loadAttachment(globalFsId, offset, limit),
															attachment.getFileName(),
															size,
															request.getHeader(HttpHeaders.RANGE)
													));
												} catch (HttpException e) {
													return Promise.<HttpResponse>ofException(e);
												}
											}));
						})
						.then(servlet -> request -> {
							try {
								Long postId = Long.parseLong(request.getPathParameter("postId"));
								request.attach("postId", postId);
								return servlet.serve(request)
										.thenEx((response, e) -> {
											if (e == null) {
												return Promise.of(response);
											} else if (e == POST_NOT_FOUND ||
													e == ThreadDao.ATTACHMENT_NOT_FOUND) {
												return Promise.of(HttpResponse.notFound404());
											} else {
												return Promise.<HttpResponse>ofException(e);
											}
										});
							} catch (NumberFormatException e) {
								return Promise.ofException(HttpException.ofCode(400,
										"'postId' path parameter should be numerical"));
							}
						})
				);
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
}

