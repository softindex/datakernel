package io.global.video.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.datakernel.ot.OTStateManager;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.CollectionUtils;
import io.datakernel.util.ref.RefByte;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOperation;
import io.global.video.ot.VideosState;
import io.global.video.pojo.VideoMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.ot.map.SetValue.set;
import static io.global.video.Utils.generateBase62;

public final class VideoDaoImpl implements VideoDao {
	private static final int VIDEO_ID_LENGTH = ApplicationSettings.getInt(VideoDaoImpl.class, "videoIdLength", 10);
	private static final Set<String> ACCEPTED_VIDEO_EXTENSIONS = CollectionUtils.set("mp4");
	private static final Set<String> ACCEPTED_THUMBNAIL_EXTENSIONS = CollectionUtils.set("png", "jpg", "jpeg");
	private static final StacklessException UNSUPPORTED_VIDEO_EXTENSION = new StacklessException(VideoDaoImpl.class, "Unsupported video extension");
	private static final StacklessException UNSUPPORTED_THUMBNAIL_EXTENSION = new StacklessException(VideoDaoImpl.class, "Unsupported thumbnail extension");

	private final FsClient fsClient;
	private final OTStateManager<CommitId, MapOperation<String, VideoMetadata>> stateManager;
	private final Map<String, VideoMetadata> stateView;

	public VideoDaoImpl(FsClient fsClient, OTStateManager<CommitId, MapOperation<String, VideoMetadata>> stateManager) {
		this.fsClient = fsClient;
		this.stateManager = stateManager;
		this.stateView = ((VideosState) stateManager.getState()).getVideos();
	}

	@Override
	public Promise<List<FileMetadata>> listAllVideos() {
		return fsClient.list("*/video.*");
	}

	@Override
	public Promise<FileMetadata> getFileMetadata(String videoId) {
		return fsClient.list(videoId + "/video.*")
				.then(list -> {
					if (list.isEmpty()) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					return fsClient.getMetadata(list.get(0).getName());
				});
	}

	@Override
	public Promise<Map<String, VideoMetadata>> listPublic() {
		return Promise.of(stateView);
	}

	@Override
	public Promise<@Nullable VideoMetadata> getVideoMetadata(String videoId) {
		return Promise.of(stateView.get(videoId));
	}

	@Override
	public Function<String, Promise<? extends ChannelConsumer<ByteBuf>>> uploadVideo() {
		RefByte refByte = new RefByte((byte) 0);
		String videoId = generateBase62(VIDEO_ID_LENGTH);
		return filename -> {
			if (filename.isEmpty()) {
				return Promise.of(ChannelConsumers.recycling());
			}
			switch (refByte.value++) {
				case 0:
					String extension = getFileExtension(filename);
					if (!ACCEPTED_VIDEO_EXTENSIONS.contains(extension)) {
						return Promise.ofException(UNSUPPORTED_VIDEO_EXTENSION);
					}
					return fsClient.upload(videoId + "/video." + extension);
				case 1:
					return uploadThumbnail(videoId, filename);
				default:
					return Promise.ofException(new StacklessException("Unexpected file: " + filename));
			}
		};
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> loadThumbnail(String videoId) {
		return fsClient.list(videoId + "/thumbnail.*")
				.then(list -> {
					if (list.isEmpty()) {
						return Promise.ofException(FILE_NOT_FOUND);
					} else {
						return fsClient.download(list.get(0).getName());
					}
				});
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> uploadThumbnail(String videoId, String filename) {
		String extension = getFileExtension(filename);
		if (!ACCEPTED_THUMBNAIL_EXTENSIONS.contains(extension)) {
			return Promise.ofException(UNSUPPORTED_THUMBNAIL_EXTENSION);
		}
		return fsClient.upload(videoId + "/thumbnail." + extension);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> watchVideo(String fileName, long offset, long limit) {
		return fsClient.download(fileName, offset, limit);
	}

	@Override
	public Promise<Void> setMetadata(String videoId, VideoMetadata metadata) {
		VideoMetadata prevMetadata = stateView.get(videoId);
		stateManager.add(MapOperation.forKey(videoId, set(prevMetadata, metadata)));
		return stateManager.sync();
	}

	@Override
	public Promise<Void> removeMetadata(String videoId) {
		VideoMetadata prevMetadata = stateView.get(videoId);
		if (prevMetadata == null) {
			return Promise.complete();
		}
		stateManager.add(MapOperation.forKey(videoId, set(prevMetadata, null)));
		return stateManager.sync();
	}

	@Override
	public Promise<Void> removeVideo(String videoId) {
		return removeMetadata(videoId)
				.then($ -> fsClient.list(videoId + "/*")
						.then(list -> {
							if (list.isEmpty()) {
								return Promise.complete();
							}
							return Promises.all(list.stream()
									.map(fileMetadata -> fsClient.delete(fileMetadata.getName())));
						}));
	}

	@Nullable
	private static String getFileExtension(String filename) {
		if (filename.lastIndexOf(".") != -1 && filename.lastIndexOf(".") != 0) {
			return filename.substring(filename.lastIndexOf(".") + 1);
		}
		return null;
	}
}
