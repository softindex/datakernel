package io.global.video.dao;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.util.ApplicationSettings;
import io.global.video.pojo.VideoMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public interface VideoDao {
	int VIDEO_ID_LENGTH = ApplicationSettings.getInt(VideoDao.class, "videoIdLength", 10);

	Promise<List<FileMetadata>> listAllVideos();

	Promise<FileMetadata> getFileMetadata(String videoId);

	Promise<Map<String, VideoMetadata>> listPublic();

	Promise<@Nullable VideoMetadata> getVideoMetadata(String videoId);

	Promise<ChannelSupplier<ByteBuf>> watchVideo(String filename, long offset, long limit);

	Promise<ChannelConsumer<ByteBuf>> uploadVideo(String videoId, @Nullable String extension);

	Promise<ChannelSupplier<ByteBuf>> loadThumbnail(String videoId);

	Promise<ChannelConsumer<ByteBuf>> uploadThumbnail(String videoId, @Nullable String extension);

	Promise<Void> setMetadata(String videoId, VideoMetadata metadata);

	Promise<Void> removeMetadata(String videoId);

	Promise<Void> removeVideo(String videoId);
}
