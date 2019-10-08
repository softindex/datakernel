package io.global.comm.util;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTStateManager;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class OTPagedAsyncMap<K, V> implements PagedAsyncMap<K, V> {
	private final OTStateManager<CommitId, MapOperation<K, V>> stateManager;
	private final Map<K, V> view;

	@Nullable
	private final Comparator<Entry<K, V>> comparator;

	public OTPagedAsyncMap(OTStateManager<CommitId, MapOperation<K, V>> stateManager, Map<K, V> view, @Nullable Comparator<Entry<K, V>> comparator) {
		this.stateManager = stateManager;
		this.comparator = comparator;
		this.view = view;
	}

	public OTPagedAsyncMap(OTStateManager<CommitId, MapOperation<K, V>> stateManager, @Nullable Comparator<Entry<K, V>> comparator) {
		this(stateManager, ((MapOTState<K, V>) stateManager.getState()).getMap(), comparator);
	}

	public OTPagedAsyncMap(OTStateManager<CommitId, MapOperation<K, V>> stateManager) {
		this(stateManager, null);
	}

	@Override
	public Promise<Map<K, V>> get() {
		return Promise.of(view);
	}

	@Override
	public Promise<V> get(K key) {
		return Promise.of(view.get(key));
	}

	@Override
	public Promise<Void> put(K key, V value) {
		V old = view.get(key);
		if (Objects.equals(value, old)) {
			return Promise.complete();
		}
		stateManager.add(MapOperation.forKey(key, SetValue.set(old, value)));
		return stateManager.sync();
	}

	@Override
	public Promise<Void> remove(K key) {
		return put(key, null);
	}

	@Override
	public Promise<Integer> size() {
		return Promise.of(view.size());
	}

	@Override
	public Promise<List<Entry<K, V>>> slice(int offset, int size) {
		Stream<Map.Entry<K, V>> stream = view.entrySet().stream();
		if (comparator != null) {
			stream = stream.sorted(comparator);
		}
		return Promise.of(stream
				.skip(offset)
				.limit(size)
				.collect(toList()));
	}

}
