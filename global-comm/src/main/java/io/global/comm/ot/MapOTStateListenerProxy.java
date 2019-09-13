package io.global.comm.ot;

import io.datakernel.ot.OTState;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class MapOTStateListenerProxy<K, V> implements OTState<MapOperation<K, V>> {
	private final MapOTState<K, V> state = new MapOTState<>(new HashMap<>());

	@Nullable
	private Consumer<MapOperation<K, V>> listener;

	public void onOperationReceived(@Nullable Consumer<MapOperation<K, V>> listener) {
		this.listener = listener;
	}

	@Override
	public void init() {
		state.init();
	}

	@Override
	public void apply(MapOperation<K, V> op) {
		state.apply(op);
		if (listener != null) {
			listener.accept(op);
		}
	}

	public Map<K, V> getMap() {
		return state.getMap();
	}
}
