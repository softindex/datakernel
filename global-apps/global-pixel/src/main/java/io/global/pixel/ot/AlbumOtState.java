package io.global.pixel.ot;

import io.datakernel.ot.OTState;
import io.global.pixel.ot.operation.AlbumOperation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class AlbumOtState implements OTState<AlbumOperation> {
	private Map<String, Album> albumMap = new LinkedHashMap<>();

	@Override
	public void init() {
		albumMap.clear();
	}

	@Override
	public void apply(AlbumOperation op) {
		op.apply(albumMap);
	}

	public Map<String, Album> getMap() {
		return Collections.unmodifiableMap(albumMap);
	}
}
