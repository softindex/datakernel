package io.global.video.ot.channel;

import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.name.ChangeName;
import io.global.video.pojo.VideoMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class ChannelOTOperation {
	private final List<ChangeName> nameOps;
	private final List<ChangeName> descriptionOps;
	private final List<MapOperation<String, VideoMetadata>> metadataOps;

	public ChannelOTOperation(List<ChangeName> nameOps,
			List<ChangeName> descriptionOps,
			List<MapOperation<String, VideoMetadata>> metadataOps) {
		this.nameOps = nameOps;
		this.descriptionOps = descriptionOps;
		this.metadataOps = metadataOps;
	}

	public static ChannelOTOperation changeName(String previous, String next, long timestamp) {
		return new ChannelOTOperation(singletonList(new ChangeName(previous, next, timestamp)), emptyList(), emptyList());
	}

	public static ChannelOTOperation changeDescription(String previous, String next, long timestamp) {
		return new ChannelOTOperation(emptyList(), singletonList(new ChangeName(previous, next, timestamp)), emptyList());
	}

	public static ChannelOTOperation setMetadata(String id, @Nullable VideoMetadata previous, @Nullable VideoMetadata next) {
		MapOperation<String, VideoMetadata> mapOperation = MapOperation.forKey(id, SetValue.set(previous, next));
		return new ChannelOTOperation(emptyList(), emptyList(), singletonList(mapOperation));
	}

	public List<ChangeName> getNameOps() {
		return nameOps;
	}

	public List<ChangeName> getDescriptionOps() {
		return descriptionOps;
	}

	public List<MapOperation<String, VideoMetadata>> getMetadataOps() {
		return metadataOps;
	}
}
