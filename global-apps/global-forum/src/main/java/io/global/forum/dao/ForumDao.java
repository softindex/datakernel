package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.util.ApplicationSettings;
import io.global.forum.ot.forum.ForumState;
import io.global.forum.pojo.ThreadMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface ForumDao {
	int VIDEO_ID_LENGTH = ApplicationSettings.getInt(ForumDao.class, "videoIdLength", 10);

	Promise<List<FileMetadata>> listAllVideos();

	Promise<FileMetadata> getFileMetadata(String videoId);

	Promise<ForumState> getChannelState();

	Promise<@Nullable ThreadMetadata> getVideoMetadata(String videoId);

	Promise<ChannelSupplier<ByteBuf>> watchVideo(String filename, long offset, long limit);

	Promise<ChannelConsumer<ByteBuf>> uploadVideo(String videoId, @Nullable String extension);

	Promise<ChannelSupplier<ByteBuf>> loadThumbnail(String videoId);

	Promise<ChannelConsumer<ByteBuf>> uploadThumbnail(String videoId, @Nullable String extension);

	Promise<Void> setMetadata(String videoId, ThreadMetadata metadata);

	Promise<Void> removeMetadata(String videoId);

	Promise<Void> removeVideo(String videoId);

	Promise<Void> updateChannelInfo(String name, String description);
}
