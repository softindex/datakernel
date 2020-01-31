package io.datakernel.vlog.view;

import io.global.comm.pojo.ThreadMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public final class VideoView implements Comparable<VideoView> {
	private final String id;
	private final ThreadMetadata meta;
	private final VideoHeaderView header;
	private final List<CommentView> comments;

	public VideoView(String id, ThreadMetadata meta, VideoHeaderView header, List<CommentView> comments) {
		this.id = id;
		this.meta = meta;
		this.header = header;
		this.comments = comments;
	}

	public String getId() {
		return id;
	}

	public ThreadMetadata getMeta() {
		return meta;
	}

	public List<CommentView> getComments() {
		return comments;
	}

	public VideoHeaderView getHeader() {
		return header;
	}

	@Nullable
	public String getDeletedBy() {
		return header.getDeletedBy();
	}

	@Override
	public int compareTo(@NotNull VideoView another) {
		return Long.compare(header.getInitialTimestampValue(), another.getHeader().getInitialTimestampValue());
	}
}
