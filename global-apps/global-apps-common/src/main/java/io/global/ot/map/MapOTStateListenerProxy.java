package io.global.ot.map;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.Consumer;

public final class MapOTStateListenerProxy<K, V> extends MapOTState<K, V> {
	@Nullable
	private Consumer<MapOperation<K, V>> listener;

	public MapOTStateListenerProxy() {
	}

	public MapOTStateListenerProxy(Map<K, V> map) {
		super(map);
	}

	public void onOperationReceived(@Nullable Consumer<MapOperation<K, V>> listener) {
		this.listener = listener;
	}

	@Override
	public void apply(MapOperation<K, V> op) {
		super.apply(op);
		if (listener != null) {
			listener.accept(op);
		}
	}
}
