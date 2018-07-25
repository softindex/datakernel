package io.datakernel.ot;

import io.datakernel.annotation.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public final class OTCommit<K, D> implements Comparable<OTCommit> {
	private final K id;
	private final Map<K, List<D>> parents;
	private final long level;

	private long timestamp;
	@Nullable
	private Boolean snapshotHint;
	@Nullable
	private Object serializedData;

	private OTCommit(K id, Map<K, List<D>> parents, long level) {
		this.id = id;
		this.parents = parents;
		this.level = level;
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> of(K id, Map<K, ? extends List<? extends D>> parents, long level) {
		checkNotNull(id);
		checkArgument(level, v -> v > 0);
		return new OTCommit<K, D>(id, (Map) parents, level);
	}

	public static <K, D> OTCommit<K, D> ofRoot(K id) {
		checkNotNull(id);
		return new OTCommit<>(id, emptyMap(), 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofCommit(K id, K parent, List<? extends D> diffs, long parentLevel) {
		checkNotNull(id);
		checkNotNull(parent);
		checkArgument(parentLevel, v -> v > 0);
		return new OTCommit<K, D>(id, (Map) singletonMap(parent, diffs), parentLevel + 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofMerge(K id, Map<K, ? extends List<? extends D>> parents, long maxParentLevel) {
		checkNotNull(id);
		checkArgument(maxParentLevel, v -> v > 0);
		return new OTCommit<K, D>(id, (Map) parents, maxParentLevel + 1L);
	}

	public OTCommit<K, D> withTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	public OTCommit<K, D> withSnapshotHint(@Nullable Boolean snapshotHint) {
		this.snapshotHint = snapshotHint;
		return this;
	}

	public OTCommit<K, D> withSerializedData(Object serializedData) {
		this.serializedData = serializedData;
		return this;
	}

	public boolean isRoot() {
		return parents.isEmpty();
	}

	public boolean isMerge() {
		return parents.size() > 1;
	}

	public boolean isCommit() {
		return parents.size() == 1;
	}

	public long getLevel() {
		return level;
	}

	public K getId() {
		return id;
	}

	public Map<K, List<D>> getParents() {
		return parents;
	}

	public Set<K> getParentIds() {
		return parents.keySet();
	}

	@Nullable
	public Boolean getSnapshotHint() {
		return snapshotHint;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Instant getInstant() {
		checkState(timestamp != 0L);
		return Instant.ofEpochMilli(timestamp);
	}

	public Object getSerializedData() {
		return serializedData;
	}

	public void setSerializedData(Object serializedData) {
		this.serializedData = serializedData;
	}

	@Override
	public String toString() {
		return "{id=" + id + ", parents=" + getParentIds() + "}";
	}

	@Override
	public int compareTo(OTCommit o) {
		return Long.compare(this.level, o.level);
	}
}
