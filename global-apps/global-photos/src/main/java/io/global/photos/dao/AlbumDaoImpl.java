package io.global.photos.dao;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.FsClient;
import io.global.ot.api.CommitId;
import io.global.ot.value.ChangeValue;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.ot.Album;
import io.global.photos.ot.Photo;
import io.global.photos.ot.operation.AlbumAddPhotoOperation;
import io.global.photos.ot.operation.AlbumChangePhotoOperation;
import io.global.photos.ot.operation.AlbumOperation;
import io.global.photos.util.ImageUtil;
import io.global.photos.util.Utils;
import org.imgscalr.Scalr.Method;
import org.jetbrains.annotations.Nullable;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.process.Cancellable.CLOSE_EXCEPTION;
import static io.global.photos.util.ImageUtil.UNSUPPORTED_FORMAT;
import static io.global.photos.util.ImageUtil.fileFormat;
import static io.global.photos.util.Utils.limitedSupplier;
import static java.util.Collections.emptyMap;

public class AlbumDaoImpl implements AlbumDao {
	private static final Method DEFAULT_INTERPOLATION_METHOD = Method.SPEED;
	private static final String JPG = "jpg";
	private final OTStateManager<CommitId, AlbumOperation> mainStateManager;
	private final CurrentTimeProvider now = CurrentTimeProvider.ofSystem();
	private final Map<String, ThumbnailDesc> thumbnails = new HashMap<>();
	private final Map<String, Photo> photoView;
	private final ExecutorService executor;
	private final MemSize imageLimit;
	private final MainDaoImpl mainDao;
	private final FsClient fsClient;
	private final String albumId;
	private final Album album;

	private AlbumDaoImpl(GlobalPhotosContainer container, Album album, FsClient fsClient, String albumId,
						 Map<String, ThumbnailDesc> thumbnails, ExecutorService executor, MemSize imageLimit, MainDaoImpl mainDao) {
		this.mainStateManager = container.getMainState();
		this.photoView = album.getPhotoMap();
		this.fsClient = fsClient;
		this.albumId = albumId;
		this.album = album;
		this.executor = executor;
		this.imageLimit = imageLimit;
		this.mainDao = mainDao;
		this.thumbnails.putAll(thumbnails);
	}

	public AlbumDaoImpl withThumbnail(ThumbnailDesc thumbnailDesc) {
		this.thumbnails.put(thumbnailDesc.title, thumbnailDesc);
		return this;
	}

	@Override
	public Promise<Album> getAlbum() {
		return Promise.of(album);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> loadPhoto(String thumbnail, String photoId, long offset, long limit) {
		return fsClient.download(ROOT_ALBUM + "/" + thumbnail + "/" + photoId, offset, limit)
				.thenEx((value, e) -> e == FsClient.FILE_NOT_FOUND ? Promise.ofException(PHOTO_NOT_FOUND_EXCEPTION) : Promise.of(value, e));
	}

	@Override
	public Promise<String> generatePhotoId() {
		String albumId;
		do {
			albumId = Utils.generateId();
		} while (photoView.containsKey(albumId));
		return Promise.of(albumId);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> addPhoto(String photoId, String filename, String description) {
		long now = this.now.currentTimeMillis();
		return fsClient.upload(ROOT_ALBUM + "/" + FULL_SIZED_THUMBNAIL + "/" + photoId, 0, now)
				.map(fullSizedImageConsumer -> {
					Eventloop eventloop = Eventloop.getCurrentEventloop();
					return ChannelConsumer.ofSupplier(supplier -> {
						ChannelSplitter<ByteBuf> splitter = ChannelSplitter.create(limitedSupplier(supplier, imageLimit));

						splitter.addOutput().set(fullSizedImageConsumer);
						ChannelSupplier<ByteBuf> imageSupplier = splitter.addOutput().getSupplier();

						// Cannot be inline due to a dead lock
						Promise<Tuple2<Integer, Integer>> thumbnailTask = ofThumbnailTask(eventloop, imageSupplier, filename, photoId, now);
						return splitter.getProcessCompletion()
								.then($ -> thumbnailTask)
								.thenEx((sizeTuple, e) -> {
									if (e == null || e == CLOSE_EXCEPTION) {
										Photo photo = Photo.create(description, now, filename, sizeTuple.getValue1(), sizeTuple.getValue2());
										mainStateManager.add(new AlbumAddPhotoOperation(albumId, photoId, photo, false));
										return mainStateManager.sync();
									} else {
										return removeInvalidPhoto(photoId)
												.<Void>then($1 -> Promise.ofException(e));
									}
								});
					});
				});
	}

	private Promise<Tuple2<Integer, Integer>> ofThumbnailTask(Eventloop eventloop, ChannelSupplier<ByteBuf> imageSupplier, String filename, String photoId, long now) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (InputStream stream = ChannelSuppliers.channelSupplierAsInputStream(eventloop, imageSupplier)) {
						return ImageIO.read(stream);
					}
				})
				.thenEx((image, e) -> e != null ? Promise.ofException(e) : image == null ? Promise.ofException(UNSUPPORTED_FORMAT) : Promise.of(image))
				.then(bufferedImage -> {
					String extension = fileFormat(filename, JPG);
					return Promises.all(thumbnails.entrySet().stream()
							.map(thumbnail ->
									addThumbnails(thumbnail, bufferedImage, photoId, now, extension)))
							.map($ -> new Tuple2<>(bufferedImage.getWidth(), bufferedImage.getHeight()));
				});
	}

	private Promise<Void> removeInvalidPhoto(String photoId) {
		long now = this.now.currentTimeMillis();
		return fsClient.delete(ROOT_ALBUM + "/" + FULL_SIZED_THUMBNAIL + "/" + photoId, now)
				.then($ -> Promises.all(thumbnails.keySet()
						.stream()
						.map(thumbnail -> fsClient.delete(ROOT_ALBUM + "/" + thumbnail + "/" + photoId, now))));
	}

	private Promise<Void> addThumbnails(Entry<String, ThumbnailDesc> thumbnailEntry, BufferedImage src, String photoId, long now, String format) {
		String title = thumbnailEntry.getKey();
		return fsClient.upload(ROOT_ALBUM + "/" + title + "/" + photoId, 0, now)
				.then(consumer -> {
					ThumbnailDesc thumbnailDesc = thumbnailEntry.getValue();
					Tuple2<Integer, Integer> scaledDimension = ImageUtil.getScaledDimension(src.getWidth(), src.getHeight(), thumbnailDesc.getDimension());
					return Promise.ofBlockingCallable(executor, () -> ImageUtil.resize(src, scaledDimension.getValue1(), scaledDimension.getValue2(), format, thumbnailDesc.getMethod()))
								.then(buf -> consumer.accept(buf, null));
				});
	}

	@Override
	public Promise<@Nullable Photo> getPhoto(String fileName) {
		return Promise.of(photoView.get(fileName));
	}

	@Override
	public Promise<Map<String, Photo>> getPhotos() {
		return Promise.of(photoView);
	}

	@Override
	public Promise<Void> removePhoto(String id) {
		Photo prev = photoView.get(id);
		long now = this.now.currentTimeMillis();
		if (photoView.containsKey(id)) {
			return fsClient.delete(albumId + "/" + FULL_SIZED_THUMBNAIL + "/" + id, now)
					.then($ -> Promises.all(thumbnails.keySet().stream()
							.map(thumbnail -> fsClient.delete(albumId + "/" + thumbnail + "/" + id))))
					.then($ -> {
						mainStateManager.add(new AlbumAddPhotoOperation(albumId, id, prev, true));
						return albumId.equals(ROOT_ALBUM) ? mainDao.getAlbums() : Promise.<Map<String, Album>>of(emptyMap());
					})
					.then(albums -> Promises.all(albums.keySet()
							.stream()
							.map(albumId -> {
								AlbumDao albumDao = mainDao.getAlbumDao(albumId);
								return albumDao == null ?
										Promise.complete() :
										albumDao.removePhoto(id);
							})))
					.then($ -> mainStateManager.sync());
		}
		return Promise.complete();
	}

	@Override
	public Promise<Void> removePhotos(Set<String> ids) {
		long now = this.now.currentTimeMillis();
		return Promises.all(ids.stream().map(id -> {
			Photo prev = photoView.get(id);
			if (photoView.containsKey(id)) {
				return fsClient.delete(albumId + "/" + FULL_SIZED_THUMBNAIL + "/" + id, now)
						.then($ -> Promises.all(thumbnails.keySet().stream()
								.map(thumbnail -> fsClient.delete(albumId + "/" + thumbnail + "/" + id, now))))
						.then($ -> {
							mainStateManager.add(new AlbumAddPhotoOperation(albumId, id, prev, true));
							return albumId.equals(ROOT_ALBUM) ? mainDao.getAlbums() : Promise.<Map<String, Album>>of(emptyMap());
						})
						.then(albums -> Promises.all(albums.keySet()
								.stream()
								.map(albumId -> {
									AlbumDao albumDao = mainDao.getAlbumDao(albumId);
									return albumDao == null ?
											Promise.complete() :
											albumDao.photoExists(id)
													.then(res -> res ? albumDao.removePhoto(id) : Promise.complete());
								}))
								.then($ -> mainStateManager.sync()));
			}
			return Promise.complete();
		}));
	}

	@Override
	public Promise<Void> removeAllPhotos() {
		return removePhotos(photoView.keySet());
	}

	@Override
	public Promise<Boolean> photoExists(String id) {
		return Promise.of(photoView.containsKey(id));
	}

	@Override
	public Promise<Long> photoSize(String thumbnail, String id) {
		return fsClient.getMetadata(ROOT_ALBUM + "/" + thumbnail + "/" + id)
				.then(fileMetadata -> fileMetadata == null ?
						Promise.ofException(PHOTO_NOT_FOUND_EXCEPTION) :
						Promise.of(fileMetadata.getSize()));
	}

	@Override
	public Promise<Void> updatePhoto(String photoId, String description) {
		Photo photo = photoView.get(photoId);
		if (photo == null) {
			return Promise.ofException(PHOTO_NOT_FOUND_EXCEPTION);
		}
		long now = this.now.currentTimeMillis();
		mainStateManager.add(new AlbumChangePhotoOperation(albumId, photoId, ChangeValue.of(photo.getDescription(), description, now)));
		return mainStateManager.sync();
	}

	public static class ThumbnailDesc {
		private final String title;
		private final Dimension dimension;
		private final Method method;

		public ThumbnailDesc(String title, Dimension dimension, Method method) {
			this.title = title;
			this.dimension = dimension;
			this.method = method;
		}

		public static ThumbnailDesc create(String title, int x, int y) {
			return create(title, new Dimension(x, y), DEFAULT_INTERPOLATION_METHOD);
		}

		public static ThumbnailDesc create(String title, Dimension dimension) {
			return create(title, dimension, DEFAULT_INTERPOLATION_METHOD);
		}

		public static ThumbnailDesc create(String title, Dimension dimension, Method method) {
			return new ThumbnailDesc(title, dimension, method);
		}

		public String getTitle() {
			return title;
		}

		public Dimension getDimension() {
			return dimension;
		}

		public Method getMethod() {
			return method;
		}
	}

	public static class Builder {
		private final MemSize memSize;
		private final ExecutorService executor;
		private final Map<String, ThumbnailDesc> thumbnails;

		public Builder(MemSize memSize, ExecutorService executor, Map<String, ThumbnailDesc> thumbnails) {
			this.memSize = memSize;
			this.executor = executor;
			this.thumbnails = thumbnails;
		}

		public AlbumDaoImpl build(GlobalPhotosContainer container, Album album, String albumId, FsClient fsClient, MainDaoImpl mainDao) {
			return new AlbumDaoImpl(container, album, fsClient, albumId, thumbnails, executor, memSize, mainDao);
		}
	}
}
