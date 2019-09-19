package io.global.ot.shared;

import java.util.Set;

public interface SharedReposOperation {
	void apply(Set<SharedRepo> repos);

	String getId();

	boolean isEmpty();

	SharedReposOperation invert();
}
