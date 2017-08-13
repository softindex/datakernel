package io.datakernel.ot;

import io.datakernel.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public final class OTCommit<K, D> {
	private final K id;

	@Nullable
	private final List<D> checkpoint;

	private final Map<K, List<D>> parents;

	private OTCommit(K id, List<D> checkpoint, Map<K, List<D>> parents) {
		this.id = id;
		this.checkpoint = checkpoint;
		this.parents = parents;
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> of(K id, List<? extends D> checkpoint, Map<K, ? extends List<? extends D>> parents) {
		return new OTCommit<K, D>(id, (List) checkpoint, (Map) parents);
	}

	public static <K, D> OTCommit<K, D> ofRoot(K id) {
		return of(id, Collections.<D>emptyList(), Collections.<K, List<D>>emptyMap());
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofCommit(K id, K parent, List<? extends D> diffs) {
		return of(id, null, (Map) singletonMap(parent, diffs));
	}

	public static <K, D> OTCommit<K, D> ofMerge(K id, Map<K, ? extends List<? extends D>> parents) {
		return of(id, null, parents);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofCheckpoint(K id, K parent, List<? extends D> checkpoint) {
		return of(id, checkpoint, (Map) singletonMap(parent, Collections.<D>emptyList()));
	}

	public boolean isRoot() {
		return parents.isEmpty();
	}

	public boolean isCheckpoint() {
		return checkpoint != null || isRoot();
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

	@Nullable
	public List<D> getCheckpoint() {
		return checkpoint;
	}

	public Map<K, List<D>> getParents() {
		return parents;
	}

	@Override
	public String toString() {
		return "{" +
				id + ":" +
				(checkpoint != null ? " checkpoint=" + checkpoint : "") +
				" " + parents +
				'}';
	}
}
