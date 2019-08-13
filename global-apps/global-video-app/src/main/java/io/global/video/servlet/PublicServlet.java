package io.global.video.servlet;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.common.PubKey;
import io.global.ot.service.ContainerHolder;
import io.global.video.container.VideoUserContainer;
import io.global.video.dao.VideoDao;
import io.global.video.pojo.UserId;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.Utils.nullToEmpty;
import static io.global.video.Utils.templated;
import static io.global.video.pojo.AuthService.DK_APP_STORE;

public final class PublicServlet {

	public static AsyncServlet create(ContainerHolder<VideoUserContainer> containerHolder, MustacheFactory mustacheFactory) {
		Mustache videoListView = mustacheFactory.compile("public/videoList.html");
		Mustache videoView = mustacheFactory.compile("public/viewVideo.html");

		return RoutingServlet.create()
				// region views
				.map(GET, "/", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					return videoDao.listPublic()
							.map(publicVideos -> templated(videoListView, publicVideos.entrySet()));
				})
				.map(GET, "/:videoId", request -> {
					VideoUserContainer container = request.getAttachment(VideoUserContainer.class);
					VideoDao videoDao = container.getVideoDao();
					String videoId = request.getPathParameter("videoId");

					return videoDao.getVideoMetadata(videoId)
							.then(metadata -> {
								if (metadata == null) {
									return Promise.<HttpResponse>ofException(HttpException.notFound404());
								}
								return container.getCommentDao(videoId)
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
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");

					return videoDao.getVideoMetadata(videoId)
							.then(metadata -> {
								if (metadata == null) {
									return Promise.<HttpResponse>ofException(HttpException.notFound404());
								}
								return videoDao.getFileMetadata(videoId)
										.then(fileMetadata -> {
											String filename = fileMetadata.getName();
											try {
												return Promise.of(HttpResponse.file((offset, limit) ->
																videoDao.watchVideo(filename, offset, limit),
														filename,
														fileMetadata.getSize()));
											} catch (HttpException e) {
												return Promise.<HttpResponse>ofException(e);
											}
										});
							});
				})
				.map(GET, "/:videoId/thumbnail", request -> {
					VideoDao videoDao = request.getAttachment(VideoUserContainer.class).getVideoDao();
					String videoId = request.getPathParameter("videoId");

					return videoDao.getVideoMetadata(videoId)
							.then(metadata -> {
								if (metadata == null) {
									return Promise.<HttpResponse>ofException(HttpException.notFound404());
								}
								return videoDao.loadThumbnail(videoId)
										.map(thumbnailStream -> HttpResponse.ok200()
												.withBodyStream(thumbnailStream));
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
					return container.getCommentDao(videoId)
							.then(commentDao -> {
								if (commentDao == null) {
									return Promise.ofException(HttpException.notFound404());
								}
								UserId userId = new UserId(DK_APP_STORE, author);
								return commentDao.addComment(userId, content);
							})
							.map($ -> {
								String pubKey = request.getPathParameter("pubKey");
								return HttpResponse.redirect302("/" + pubKey + "/" + videoId + "/");
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
