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
import io.global.video.Utils;
import io.global.video.container.VideoUserContainer;
import io.global.video.dao.ChannelDao;
import io.global.video.pojo.VideoMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.MultipartParser.MultipartDataHandler.fieldsToMap;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.video.Utils.*;
import static io.global.video.dao.ChannelDao.VIDEO_ID_LENGTH;
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
					ChannelDao channelDao = container.createChannelDao();
					return channelDao.listAllVideos()
							.then(allVideos -> channelDao.getChannelState()
									.map(state -> {
										Set<String> videoIds = state.getMetadata().keySet();
										Set<String> untagged = allVideos.stream()
												.map(fileMetadata -> {
													String fileName = fileMetadata.getName();
													return fileName.substring(0, fileName.indexOf("/"));
												})
												.filter(id -> !videoIds.contains(id))
												.collect(toSet());
										return templated(videoListView, map(
												"public key", container.getKeys().getPubKey().asString(),
												"name", state.getName(),
												"description", state.getDescription(),
												"metadata", state.getMetadata().entrySet(),
												"untagged", untagged));
									}));
				})
				.map(HttpMethod.GET, "/:videoId/", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					ChannelDao channelDao = container.createChannelDao();
					String videoId = request.getPathParameter("videoId");

					//noinspection ConstantConditions
					return channelDao.getVideoMetadata(videoId)
							.then(metadata -> container.createCommentDao(videoId)
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
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");

					return channelDao.getFileMetadata(videoId)
							.then(fileMetadata -> {
								String fileName = fileMetadata.getName();
								try {
									return Promise.of(HttpResponse.file((offset, limit) ->
													channelDao.watchVideo(fileName, offset, limit),
											fileName,
											fileMetadata.getSize()));
								} catch (HttpException e) {
									return Promise.<HttpResponse>ofException(e);
								}
							});
				})
				.map(HttpMethod.GET, "/:videoId/thumbnail", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");

					return channelDao.loadThumbnail(videoId)
							.mapEx((thumbnailStream, e) -> {
								if (e == FILE_NOT_FOUND) {
									return HttpResponse.redirect302("/no-thumbnail");
								}
								return HttpResponse.ok200()
										.withBodyStream(thumbnailStream);
							});
				})
				.map(POST, "/upload", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = generateBase62(VIDEO_ID_LENGTH);
					Map<String, String> fields = new HashMap<>();
					return request
							.handleMultipart(fieldsToMap(fields, (fieldName, fileName) -> {
								switch (fieldName) {
									case "video":
										if (fileName.isEmpty()) {
											return Promise.ofException(new StacklessException(OwnerServlet.class, "Video file is required"));
										}
										return channelDao.uploadVideo(videoId, getFileExtension(fileName));
									case "thumbnail":
										if (fileName.isEmpty()) {
											return Promise.of(ChannelConsumers.recycling());
										}
										return channelDao.uploadThumbnail(videoId, getFileExtension(fileName));
									default:
										return Promise.ofException(new StacklessException(OwnerServlet.class, "Unsupported file field"));
								}
							}))
							.then($ -> {
								if (fields.isEmpty()) {
									return Promise.complete();
								}
								return extractMetadata(fields)
										.then(metadata -> channelDao.setMetadata(videoId, metadata));
							})
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(POST, "/:videoId/update", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");

					return extractMetadata(request.getPostParameters())
							.then(metadata -> channelDao.setMetadata(videoId, metadata))
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(POST, "/:videoId/deleteMetadata/", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");
					return channelDao.removeMetadata(videoId)
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(POST, "/:videoId/delete", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");
					return channelDao.removeVideo(videoId)
							.map($ -> HttpResponse.redirect302("/myChannel"));
				})
				.map(POST, "/:videoId/deleteComment", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					String videoId = request.getPathParameter("videoId");
					String commentIdString = request.getPostParameter("commentId");
					if (commentIdString == null) {
						return Promise.ofException(HttpException.ofCode(400,
								"'commentIdString' parameter is required"));
					}

					try {
						Long commentId = Long.valueOf(commentIdString);
						return container.createCommentDao(videoId)
								.then(commentDao -> {
									if (commentDao == null) {
										return Promise.<HttpResponse>ofException(HttpException.notFound404());
									}
									return commentDao.removeComments(singleton(commentId))
											.map($ -> Utils.redirect(request, "./"));
								});
					} catch (NumberFormatException e) {
						return Promise.ofException(HttpException.ofCode(400, "Invalid `commentId`"));
					}
				})
				.map(POST, "/updateInfo", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String name = request.getPostParameter("name");
					String description = request.getPostParameter("description");
					if (name == null || description == null) {
						return Promise.ofException(HttpException.ofCode(400, "'name' and 'description' are required"));
					}
					return channelDao.updateChannelInfo(name, description)
							.map($ -> HttpResponse.redirect302("/myChannel"));
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
