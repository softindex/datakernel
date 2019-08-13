package io.global.video.ot;

import io.datakernel.ot.OTState;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.video.pojo.VideoMetadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class VideosState implements OTState<MapOperation<String, VideoMetadata>> {
	private final MapOTState<String, VideoMetadata> state = new MapOTState<>(new LinkedHashMap<>());
	private Consumer<MapOperation<String, VideoMetadata>> listener;

	public void setListener(Consumer<MapOperation<String, VideoMetadata>> listener) {
		this.listener = listener;
	}

	@Override
	public void init() {
		state.init();
	}

	@Override
	public void apply(MapOperation<String, VideoMetadata> op) {
		state.apply(op);
		if (listener != null) {
			listener.accept(op);
		}
	}

	public Map<String, VideoMetadata> getVideos() {
		return state.getMap();
	}
}
