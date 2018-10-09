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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.singletonMap;

public final class OTCommit<K, D> {
	private final K id;
	private final Map<K, List<D>> parents;

	private boolean snapshot;
	private long timestamp;

	private OTCommit(K id, Map<K, List<D>> parents) {
		this.id = id;
		this.parents = parents;
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> of(K id, Map<K, ? extends List<? extends D>> parents) {
		checkArgument(id != null, "Cannot create OTCommit with id that is null");
		checkArgument(parents != null, "Cannot create OTCommit with parents that is null");
		return new OTCommit<K, D>(id, (Map) parents);
	}

	public static <K, D> OTCommit<K, D> ofRoot(K id) {
		return of(id, Collections.<K, List<D>>emptyMap());
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofCommit(K id, K parent, List<? extends D> diffs) {
		return of(id, (Map) singletonMap(parent, diffs));
	}

	public static <K, D> OTCommit<K, D> ofMerge(K id, Map<K, ? extends List<? extends D>> parents) {
		return of(id, parents);
	}

	public OTCommit<K, D> withCommitMetadata(long timestamp, boolean snapshot) {
		setCommitMetadata(timestamp, snapshot);
		return this;
	}

	public void setCommitMetadata(long timestamp, boolean snapshot) {
		this.timestamp = timestamp;
		this.snapshot = snapshot;
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

	public K getId() {
		return id;
	}

	public Map<K, List<D>> getParents() {
		return parents;
	}

	public Set<K> getParentIds() {
		return parents.keySet();
	}

	public boolean isSnapshot() {
		return snapshot;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "{id=" + id + ", parents=" + getParentIds() + "}";
	}
}
