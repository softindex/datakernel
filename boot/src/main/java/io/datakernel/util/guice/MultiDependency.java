package io.datakernel.util.guice;

import com.google.inject.Inject;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

public class MultiDependency<T> {
	@Inject(optional = true)
	@Nullable
	private T value;

	@Inject(optional = true)
	@Nullable
	private Set<T> values;

	public Set<T> getValues() {
		Set<T> set = values != null ? new HashSet<>(values) : new HashSet<>();
		if (value != null) {
			set.add(value);
		}
		return set;
	}
}
