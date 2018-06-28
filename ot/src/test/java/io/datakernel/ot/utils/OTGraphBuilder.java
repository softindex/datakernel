package io.datakernel.ot.utils;

import java.util.List;

import static java.util.Arrays.asList;

public interface OTGraphBuilder<K, D> {
	void add(K parent, K child, List<D> diffs);

	default void add(K parent, K child, D diff) {
		add(parent, child, asList(diff));
	}
}
