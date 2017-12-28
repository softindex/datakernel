package io.datakernel.ot;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;

public final class OTCommit<K, D> {
	private final K id;

	private final Map<K, List<D>> parents;

	private OTCommit(K id, Map<K, List<D>> parents) {
		this.id = id;
		this.parents = parents;
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> of(K id, Map<K, ? extends List<? extends D>> parents) {
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

	@Override
	public String toString() {
		return "{" + id + ": " + parents + '}';
	}

	public String idsToString() {
		return "{" + id + ": " + getParentIds() + '}';
	}
}
