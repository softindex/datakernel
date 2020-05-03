package io.datakernel.di.util;

import io.datakernel.di.binding.Binding;
import io.datakernel.di.binding.BindingType;

import static io.datakernel.di.binding.BindingType.EAGER;
import static io.datakernel.di.binding.BindingType.TRANSIENT;

/**
 * A container that groups together a bindings and its type, only for internal use
 */
public final class MarkedBinding<K> {
	private final Binding<K> binding;
	private final BindingType type;

	public MarkedBinding(Binding<K> binding, BindingType type) {
		this.binding = binding;
		this.type = type;
	}

	public Binding<K> getBinding() {
		return binding;
	}

	public BindingType getType() {
		return type;
	}

	@Override
	public String toString() {
		return (type == TRANSIENT ? "*" : type == EAGER ? "!" : "") + binding.toString();
	}
}
