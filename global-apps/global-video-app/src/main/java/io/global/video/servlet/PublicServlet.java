package io.global.video.servlet;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.common.PubKey;
import io.global.ot.service.ContainerHolder;
import io.global.video.container.VideoUserContainer;
import io.global.video.dao.ChannelDao;
import io.global.video.pojo.UserId;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.Utils.nullToEmpty;
import static io.global.video.Utils.redirect;
import static io.global.video.Utils.templated;
import static io.global.video.pojo.AuthService.DK_APP_STORE;

public final class PublicServlet {

	public static AsyncServlet create(ContainerHolder<VideoUserContainer> containerHolder, MustacheFactory mustacheFactory) {
		Mustache videoListView = mustacheFactory.compile("public/videoList.html");
		Mustache videoView = mustacheFactory.compile("public/viewVideo.html");

		return RoutingServlet.create()
				// region views
				.map(GET, "/", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					return channelDao.getChannelState()
							.map(channelState -> templated(videoListView, map("name", channelState.getName(),
									"description", channelState.getDescription(),
									"videos", channelState.getMetadata().entrySet())));
				})
				.map(GET, "/:videoId", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					ChannelDao channelDao = container.createChannelDao();
					String videoId = request.getPathParameter("videoId");

					return channelDao.getVideoMetadata(videoId)
							.then(metadata -> {
								if (metadata == null) {
									return Promise.<HttpResponse>ofException(HttpException.notFound404());
								}
								return container.createCommentDao(videoId)
										.then(commentDao -> commentDao == null ?
												Promise.ofException(HttpException.notFound404()) :
												commentDao.listComments())
										.map(comments -> templated(videoView, map(
												"videoId", videoId,
												"metadata", metadata,
												"comments", comments.entrySet())));
							});
				})
				// endregion

				// region API
				.map(GET, "/:videoId/watch", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");

					return channelDao.getVideoMetadata(videoId)
							.then(metadata -> {
								if (metadata == null) {
									return Promise.<HttpResponse>ofException(HttpException.notFound404());
								}
								return channelDao.getFileMetadata(videoId)
										.then(fileMetadata -> {
											String filename = fileMetadata.getName();
											try {
												return Promise.of(HttpResponse.file((offset, limit) ->
																channelDao.watchVideo(filename, offset, limit),
														filename,
														fileMetadata.getSize()));
											} catch (HttpException e) {
												return Promise.<HttpResponse>ofException(e);
											}
										});
							});
				})
				.map(GET, "/:videoId/thumbnail", request -> {
					ChannelDao channelDao = request.getAttachment(VideoUserContainer.class).createChannelDao();
					String videoId = request.getPathParameter("videoId");

					return channelDao.getVideoMetadata(videoId)
							.then(metadata -> {
								if (metadata == null) {
									return Promise.<HttpResponse>ofException(HttpException.notFound404());
								}
								return channelDao.loadThumbnail(videoId)
										.mapEx((thumbnailStream, e) -> {
											if (e == FILE_NOT_FOUND) {
												return HttpResponse.redirect302("/no-thumbnail");
											}
											return HttpResponse.ok200()
													.withBodyStream(thumbnailStream);
										});
							});
				})
				.map(POST, "/:videoId/addComment", request -> {
					String videoId = request.getPathParameter("videoId");
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					String author = nullToEmpty(request.getPostParameter("author"));
					String content = nullToEmpty(request.getPostParameter("content"));
					if (author.isEmpty() || content.isEmpty()) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return container.createCommentDao(videoId)
							.then(commentDao -> {
								if (commentDao == null) {
									return Promise.ofException(HttpException.notFound404());
								}
								UserId userId = new UserId(DK_APP_STORE, author);
								return commentDao.addComment(userId, content);
							})
							.map($ -> {
								String pubKey = request.getPathParameter("pubKey");
								return redirect(request, "/user/" + pubKey + "/" + videoId + "/");
							});
				})
				// endregion
				.then(getContainerDecorator(containerHolder));
	}

	private static AsyncServletDecorator getContainerDecorator(ContainerHolder<VideoUserContainer> containerHolder) {
		return servlet ->
				request -> {
					try {
						String key = request.getPathParameter("pubKey");
						PubKey pubKey = PubKey.fromString(key);
						return containerHolder.getUserContainer(pubKey)
								.then(container -> {
									if (container == null) {
										return Promise.ofException(HttpException.notFound404());
									}
									request.attach(container);
									return servlet.serve(request);
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				};
	}

}
