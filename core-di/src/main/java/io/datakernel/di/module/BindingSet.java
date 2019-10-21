package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.DIException;
import io.datakernel.di.core.Key;

import java.util.HashSet;
import java.util.Set;

public final class BindingSet<K> {
	private final Set<Binding<K>> bindings;
	private final BindingType type;

	public BindingSet(Set<Binding<K>> bindings, BindingType type) {
		this.bindings = bindings;
		this.type = type;
	}

	public Set<Binding<K>> getBindings() {
		return bindings;
	}

	public BindingType getType() {
		return type;
	}

	public enum BindingType {
		COMMON, TRANSIENT, EAGER
	}

	@SuppressWarnings("unchecked")
	public static BindingSet<?> merge(Key<?> key, BindingSet<?> first, BindingSet<?> second) {
		if (first.type != second.type) {
			throw new DIException("Two binding sets bound with different types for key " + key.getDisplayString());
		}
		Set<Binding<?>> set = new HashSet<>();
		set.addAll(first.bindings);
		set.addAll(second.bindings);
		return new BindingSet(set, first.type);
	}
}
