package io.global.video.servlet;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.common.PrivKey;
import io.global.ot.service.ContainerHolder;
import io.global.video.container.VideoUserContainer;
import io.global.video.dao.VideoDao;
import io.global.video.pojo.VideoMetadata;

import java.util.Set;

import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.video.Utils.templated;
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
				.map(HttpMethod.GET, "/watch/:videoId", request -> {
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
									return HttpResponse.redirect302("https://via.placeholder.com/150C");
								}
								return HttpResponse.ok200()
										.withBodyStream(thumbnailStream);
							});
				})
				.map(HttpMethod.POST, "/upload", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					return request
							.getFiles(videoDao.uploadVideo())
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, "/update/:videoId", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");
					String title = request.getPostParameter("title");
					String description = request.getPostParameter("description");
					if (title == null || description == null) {
						return Promise.ofException(HttpException.ofCode(400, "'title' and 'description' parameters are required"));
					}

					if (title.isEmpty()) {
						return Promise.ofException(HttpException.ofCode(400, "Title cannot be empty"));
					}

					return videoDao.getFileMetadata(videoId)
							.then(metadata -> videoDao.setMetadata(videoId, new VideoMetadata(title, description)))
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, "/deleteMetadata/:videoId", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");
					return videoDao.removeMetadata(videoId)
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(HttpMethod.POST, "/delete/:videoId", request -> {
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
