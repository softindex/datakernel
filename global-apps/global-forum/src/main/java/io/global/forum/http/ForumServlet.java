package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.datakernel.http.RoutingServlet;
import io.datakernel.util.TypeT;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.pojo.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.http.HttpMethod.*;
import static io.global.forum.Utils.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class ForumServlet {
	public static final StacklessException INSUFFICIENT_RIGHTS = new StacklessException("Your rights are not sufficient for that action");
	public static final StacklessException USER_NOT_FOUND = new StacklessException("User not found");

	public static RoutingServlet create(ForumDao forumDao) {
		return RoutingServlet.create()
				.map(GET, "/metadata", request ->
						forumDao.getForumMetadata()
								.map(metadata -> HttpResponse.ok200()
										.withJson(REGISTRY.get(ForumMetadata.class), metadata)))
				.map(POST, "/metadata", request -> {
					try {
						return forumDao.setForumMetadata(JsonUtils.fromJson(REGISTRY.get(ForumMetadata.class), request.getBody().asString(UTF_8)))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/profile", request ->
						forumDao.getUser(request.getAttachment(UserId.class))
								.then(userData -> {
									if (userData == null) {
										return Promise.<HttpResponse>ofException(USER_NOT_FOUND);
									}
									return Promise.of(HttpResponse.ok200().withJson(REGISTRY.get(UserData.class), userData));
								}))
				.map(POST, "/profile", request -> {
					try {
						return forumDao.updateUser(request.getAttachment(UserId.class), JsonUtils.fromJson(REGISTRY.get(UserData.class), request.getBody().asString(UTF_8)))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/bans", request ->
						forumDao.getUser(request.getAttachment(UserId.class))
								.then(userData -> {
									if (userData == null) {
										return Promise.<HttpResponse>ofException(USER_NOT_FOUND);
									}
									if (!userData.getRole().hasSufficientRights()) {
										return Promise.<HttpResponse>ofException(INSUFFICIENT_RIGHTS);
									}
									return forumDao.getBannedRanges()
											.map(bans -> HttpResponse.ok200().withJson(REGISTRY.get(new TypeT<Map<Long, IpBanState>>() {}), bans));
								}))
				.map(POST, "/ban", request -> {
					UserId userId = request.getAttachment(UserId.class);
					return forumDao.getUser(userId)
							.then(userData -> {
								if (userData == null) {
									return Promise.<HttpResponse>ofException(USER_NOT_FOUND);
								}
								if (!userData.getRole().hasSufficientRights()) {
									return Promise.<HttpResponse>ofException(INSUFFICIENT_RIGHTS);
								}
								try {
									IpBanRequest banRequest = JsonUtils.fromJson(REGISTRY.get(IpBanRequest.class), request.getBody().asString(UTF_8));
									return forumDao.banIpRange(banRequest.getRange(), userId, banRequest.getUntil(), banRequest.getDescription())
											.map($ -> HttpResponse.ok200());
								} catch (ParseException e) {
									return Promise.<HttpResponse>ofException(e);
								}
							});
				})
				.map(DELETE, "/ban/:id", request ->
						forumDao.getUser(request.getAttachment(UserId.class))
								.then(userData -> {
									if (userData == null) {
										return Promise.<HttpResponse>ofException(USER_NOT_FOUND);
									}
									if (!userData.getRole().hasSufficientRights()) {
										return Promise.<HttpResponse>ofException(INSUFFICIENT_RIGHTS);
									}

									return getId(request)
											.then(forumDao::unbanIpRange)
											.map($ -> HttpResponse.ok200());
								}))
				.map(GET, "/threads", request ->
						forumDao.getThreads()
								.map(threads -> HttpResponse.ok200().withJson(REGISTRY.get(new TypeT<Map<Long, ThreadMetadata>>() {}), threads)))
				.map(POST, "/thread", request -> {
					try {
						ThreadMetadata metadata = JsonUtils.fromJson(REGISTRY.get(ThreadMetadata.class), request.getBody().asString(UTF_8));
						UserId userId = request.getAttachment(UserId.class);
						return forumDao.createThread(metadata)
								.then(id -> {
									ThreadDao threadDao = forumDao.getThreadDao(id);
									assert threadDao != null : "just created thread has no dao";

									Map<String, Attachment> attachmentMap = new HashMap<>();
									Map<String, String> paramsMap = new HashMap<>();

									MultipartDataHandler handler = AttachmentDataHandler.create(threadDao, paramsMap, attachmentMap);
									return request.handleMultipart(handler)
											.then($ -> {
												String content = paramsMap.get("content");
												if (content == null) {
													return Promise.ofException(new ParseException(ThreadServlet.class, "'content' POST parameter is required"));
												}
												return threadDao.addRootPost(userId, content, attachmentMap);
											})
											.thenEx(revertIfException(forumDao, threadDao, id, attachmentMap))
											.map($ -> HttpResponse.ok201().withJson(LONG_CODEC, id));
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(DELETE, "/:id", request ->
						getId(request)
								.then(forumDao::removeThread)
								.map($ -> HttpResponse.ok200()))
				.map("/:id", ThreadServlet.create()
						.then(servlet ->
								request ->
										getId(request)
												.then(tid -> {
													request.attach(forumDao.getThreadDao(tid));
													return servlet.serve(request);
												})));
	}

	private static BiFunction<Void, Throwable, Promise<Void>> revertIfException(ForumDao forumDao, ThreadDao threadDao, long id, Map<String, Attachment> attachments) {
		return ($, e) -> {
			if (e == null) {
				return Promise.complete();
			}
			return threadDao.deleteAttachments(attachments.keySet())
					.thenEx(($2, e2) -> {
						if (e2 != null) {
							e.addSuppressed(e2);
						}
						return forumDao.removeThread(id)
								.thenEx(($3, e3) -> {
									if (e3 != null) {
										e.addSuppressed(e3);
									}
									return Promise.ofException(e);
								});
					});
		};
	}

	private static Promise<Long> getId(HttpRequest request) {
		String id = request.getPathParameter("id");
		try {
			return Promise.of(Long.parseLong(id));
		} catch (NumberFormatException e) {
			return Promise.ofException(HttpException.ofCode(401, "bad id: " + id));
		}
	}
}

