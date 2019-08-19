package io.global.video.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.datakernel.ot.OTStateManager;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.CollectionUtils;
import io.global.ot.api.CommitId;
import io.global.ot.name.ChangeName;
import io.global.video.ot.channel.ChannelOTOperation;
import io.global.video.ot.channel.ChannelState;
import io.global.video.pojo.VideoMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;

import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class ChannelDaoImpl implements ChannelDao {
	private static final Set<String> ACCEPTED_VIDEO_EXTENSIONS = CollectionUtils.set("mp4");
	private static final Set<String> ACCEPTED_THUMBNAIL_EXTENSIONS = CollectionUtils.set("png", "jpg", "jpeg");
	private static final StacklessException UNSUPPORTED_VIDEO_EXTENSION = new StacklessException(ChannelDaoImpl.class, "Unsupported video extension");
	private static final StacklessException UNSUPPORTED_THUMBNAIL_EXTENSION = new StacklessException(ChannelDaoImpl.class, "Unsupported thumbnail extension");

	private final FsClient fsClient;
	private final OTStateManager<CommitId, ChannelOTOperation> stateManager;
	private final ChannelState state;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public ChannelDaoImpl(FsClient fsClient, OTStateManager<CommitId, ChannelOTOperation> stateManager) {
		this.fsClient = fsClient;
		this.stateManager = stateManager;
		this.state = ((ChannelState) stateManager.getState());
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
	public Promise<ChannelState> getChannelState() {
		return Promise.of(state);
	}

	@Override
	public Promise<@Nullable VideoMetadata> getVideoMetadata(String videoId) {
		return Promise.of(state.getMetadata().get(videoId));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> uploadVideo(String videoId, @Nullable String extension) {
		if (!ACCEPTED_VIDEO_EXTENSIONS.contains(extension)) {
			return Promise.ofException(UNSUPPORTED_VIDEO_EXTENSION);
		}
		return fsClient.upload(videoId + "/video." + extension);
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
	public Promise<ChannelConsumer<ByteBuf>> uploadThumbnail(String videoId, @Nullable String extension) {
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
		VideoMetadata prevMetadata = state.getMetadata().get(videoId);
		return applyAndSync(ChannelOTOperation.setMetadata(videoId, prevMetadata, metadata));
	}

	@Override
	public Promise<Void> removeMetadata(String videoId) {
		VideoMetadata prevMetadata = state.getMetadata().get(videoId);
		if (prevMetadata == null) {
			return Promise.complete();
		}
		return applyAndSync(ChannelOTOperation.setMetadata(videoId, prevMetadata, null));
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

	@Override
	public Promise<Void> updateChannelInfo(String name, String description) {
		String previousName = state.getName();
		String previousDescription = state.getDescription();
		long timestamp = now.currentTimeMillis();
		return applyAndSync(new ChannelOTOperation(
				singletonList(new ChangeName(previousName, name, timestamp)),
				singletonList(new ChangeName(previousDescription, description, timestamp)),
				emptyList()
		));
	}

	private Promise<Void> applyAndSync(ChannelOTOperation channelOperation) {
		stateManager.add(channelOperation);
		return stateManager.sync();
	}

}
