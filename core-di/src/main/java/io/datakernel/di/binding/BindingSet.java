package io.datakernel.di.binding;

import io.datakernel.di.Key;
import io.datakernel.di.module.ModuleBuilder0;

import java.util.HashSet;
import java.util.Set;

/**
 * This class represents a set of bindings for the same key.
 * It also contains the {@link BindingType} - it is mostly used for plain binds with extra calls,
 * such as {@link ModuleBuilder0#asTransient transient} or {@link ModuleBuilder0#asEager eager}.
 * <p>
 * Note that one of the bindings itself may still be transient while the type of the set is {@link BindingType#REGULAR COMMON},
 * this case should be handled properly (e.g. {@link Multibinder#toSet toSet()} multibinder is transient if at least one of its peers is transient).
 */
public final class BindingSet<K> {
	private final Set<Binding<K>> bindings;
	private BindingType type;

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

	public void setType(BindingType type) {
		this.type = type;
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
