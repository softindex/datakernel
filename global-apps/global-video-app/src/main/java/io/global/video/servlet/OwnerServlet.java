package io.global.video.servlet;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.*;
import io.global.common.PrivKey;
import io.global.ot.service.ContainerHolder;
import io.global.video.container.VideoUserContainer;
import io.global.video.dao.VideoDao;
import io.global.video.pojo.VideoMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.http.MultipartParser.MultipartDataHandler.fieldsToMap;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.video.Utils.*;
import static io.global.video.dao.VideoDao.VIDEO_ID_LENGTH;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public final class OwnerServlet {

	public static AsyncServlet create(ContainerHolder<VideoUserContainer> containerHolder, MustacheFactory mustacheFactory) {
		Mustache videoListView = mustacheFactory.compile("owner/videoList.html");
		Mustache videoView = mustacheFactory.compile("owner/viewVideo.html");
		Mustache addVideoView = mustacheFactory.compile("owner/addVideo.html");

		return RoutingServlet.create()
				// region views
				.map(HttpMethod.GET, "/", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					VideoDao videoDao = container.getVideoDao();
					return videoDao.listAllVideos()
							.then(allVideos -> videoDao.listPublic()
									.map(publicVideos -> {
										Set<String> videoIds = publicVideos.keySet();
										Set<String> untagged = allVideos.stream()
												.map(fileMetadata -> {
													String fileName = fileMetadata.getName();
													return fileName.substring(0, fileName.indexOf("/"));
												})
												.filter(id -> !videoIds.contains(id))
												.collect(toSet());
										return templated(videoListView, map(
												"public key", container.getKeys().getPubKey().asString(),
												"public", publicVideos.entrySet(),
												"untagged", untagged));
									}));
				})
				.map(HttpMethod.GET, "/:videoId/", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					VideoDao videoDao = container.getVideoDao();
					String videoId = request.getPathParameter("videoId");

					//noinspection ConstantConditions
					return videoDao.getVideoMetadata(videoId)
							.then(metadata -> container.getCommentDao(videoId)
									.then(commentDao -> commentDao == null ?
											Promise.of(emptyMap()) :
											commentDao.listComments())
									.map(comments -> templated(videoView, map(
											"videoId", videoId,
											"metadata", metadata,
											"comments", comments.entrySet()))));
				})
				.map(HttpMethod.GET, "/add", request -> Promise.of(templated(addVideoView)))
				// endregion

				// region API
				.map(HttpMethod.GET, "/:videoId/watch", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");

					return videoDao.getFileMetadata(videoId)
							.then(fileMetadata -> {
								String fileName = fileMetadata.getName();
								try {
									return Promise.of(HttpResponse.file((offset, limit) ->
													videoDao.watchVideo(fileName, offset, limit),
											fileName,
											fileMetadata.getSize()));
								} catch (HttpException e) {
									return Promise.<HttpResponse>ofException(e);
								}
							});
				})
				.map(HttpMethod.GET, "/:videoId/thumbnail", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");

					return videoDao.loadThumbnail(videoId)
							.mapEx((thumbnailStream, e) -> {
								if (e == FILE_NOT_FOUND) {
									return HttpResponse.redirect302("/no-thumbnail");
								}
								return HttpResponse.ok200()
										.withBodyStream(thumbnailStream);
							});
				})
				.map(HttpMethod.POST, "/upload", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = generateBase62(VIDEO_ID_LENGTH);
					Map<String, String> fields = new HashMap<>();
					return request
							.handleMultipart(fieldsToMap(fields, (fieldName, fileName) -> {
								switch (fieldName) {
									case "video":
										if (fileName.isEmpty()) {
											return Promise.ofException(new StacklessException(OwnerServlet.class, "Video file is required"));
										}
										return videoDao.uploadVideo(videoId, getFileExtension(fileName));
									case "thumbnail":
										if (fileName.isEmpty()) {
											return Promise.of(ChannelConsumers.recycling());
										}
										return videoDao.uploadThumbnail(videoId, getFileExtension(fileName));
									default:
										return Promise.ofException(new StacklessException(OwnerServlet.class, "Unsupported file field"));
								}
							}))
							.then($ -> {
								if (fields.isEmpty()) {
									return Promise.complete();
								}
								return extractMetadata(fields)
										.then(metadata -> videoDao.setMetadata(videoId, metadata));
							})
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, "/:videoId/update", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");

					return extractMetadata(request.getPostParameters())
							.then(metadata -> videoDao.setMetadata(videoId, metadata))
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, "/:videoId/deleteMetadata/", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");
					return videoDao.removeMetadata(videoId)
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, ":videoId/delete", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");
					return videoDao.removeVideo(videoId)
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, "/:videoId/deleteComment", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					String videoId = request.getPathParameter("videoId");
					String commentIdString = request.getPostParameter("commentId");
					if (commentIdString == null) {
						return Promise.ofException(HttpException.ofCode(400,
								"'commentIdString' parameter is required"));
					}

					try {
						Long commentId = Long.valueOf(commentIdString);
						return container.getCommentDao(videoId)
								.then(commentDao -> {
									if (commentDao == null) {
										return Promise.<HttpResponse>ofException(HttpException.notFound404());
									}
									return commentDao.removeComments(singleton(commentId))
											.map($ -> HttpResponse.redirect302("./"));
								});
					} catch (NumberFormatException e) {
						return Promise.ofException(HttpException.ofCode(400, "Invalid `commentId`"));
					}
				})
				// endregion
				.then(ensureContainerDecorator(containerHolder));
	}

	private static Promise<VideoMetadata> extractMetadata(Map<String, String> params) {
		String title = params.get("title");
		String description = params.get("description");

		if (title == null || description == null) {
			return Promise.ofException(HttpException.ofCode(400, "'title' and 'description' parameters are required"));
		}

		if (title.isEmpty()) {
			return Promise.ofException(HttpException.ofCode(400, "Title cannot be empty"));
		}

		return Promise.of(new VideoMetadata(title, description));
	}

	private static AsyncServletDecorator ensureContainerDecorator(ContainerHolder<VideoUserContainer> containerHolder) {
		return servlet ->
				request -> {
					try {
						String key = request.getCookie("Key");
						if (key == null) {
							return Promise.ofException(HttpException.ofCode(401, "Cookie 'Key' is required"));
						}
						PrivKey privKey = PrivKey.fromString(key);
						return containerHolder.ensureUserContainer(privKey)
								.thenEx((container, e) -> {
									if (e == null) {
										request.attach(container);
										return servlet.serve(request);
									} else {
										return Promise.ofException(HttpException.ofCode(500, "Failed to start container", e));
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				};
	}

}
