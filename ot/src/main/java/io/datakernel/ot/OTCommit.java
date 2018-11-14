/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot;

import io.datakernel.annotation.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public final class OTCommit<K, D> implements Comparable<OTCommit<?, ?>> {
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
		checkArgument(level, v -> v > 0, "Level should be greater than 0");
		checkArgument(parents != null, "Cannot create OTCommit with parents that is null");
		return new OTCommit<>(id, (Map<K, List<D>>) parents, level);
	}

	public static <K, D> OTCommit<K, D> ofRoot(K id) {
		checkNotNull(id);
		return new OTCommit<>(id, emptyMap(), 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofCommit(K id, K parent, List<? extends D> diffs, long parentLevel) {
		checkNotNull(id);
		checkNotNull(parent);
		checkArgument(parentLevel, v -> v > 0, "Level should be greater than 0");
		Map<K, ? extends List<? extends D>> parentMap = singletonMap(parent, diffs);
		return new OTCommit<>(id, (Map<K, List<D>>) parentMap, parentLevel + 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofMerge(K id, Map<K, ? extends List<? extends D>> parents, long maxParentLevel) {
		checkNotNull(id);
		checkArgument(maxParentLevel, v -> v > 0, "Level should be greater than 0");
		return new OTCommit<>(id, (Map<K, List<D>>) parents, maxParentLevel + 1L);
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
		checkState(timestamp != 0L, "Timestamp has not been set");
		return Instant.ofEpochMilli(timestamp);
	}

	@Nullable
	public Object getSerializedData() {
		return serializedData;
	}

	public void setSerializedData(@Nullable Object serializedData) {
		this.serializedData = serializedData;
	}

	@Override
	public String toString() {
		return "{id=" + id + ", parents=" + getParentIds() + "}";
	}

	@Override
	public int compareTo(OTCommit<?, ?> o) {
		return Long.compare(this.level, o.level);
	}
}
