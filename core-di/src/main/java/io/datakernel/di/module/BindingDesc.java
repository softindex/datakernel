package io.datakernel.di.module;

import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.binding.Binding;
import io.datakernel.di.binding.BindingType;

import static io.datakernel.di.Scope.UNSCOPED;
import static io.datakernel.di.binding.BindingType.REGULAR;

public final class BindingDesc {
	private Key<?> key;
	private Binding<?> binding;
	private Scope[] scope;
	private BindingType type;

	public BindingDesc(Key<?> key, Binding<?> binding) {
		this.key = key;
		this.binding = binding;
		this.scope = UNSCOPED;
		this.type = REGULAR;
	}

	public Key<?> getKey() {
		return key;
	}

	public void setKey(Key<?> key) {
		this.key = key;
	}

	public Binding<?> getBinding() {
		return binding;
	}

	public void setBinding(Binding<?> binding) {
		this.binding = binding;
	}

	public Scope[] getScope() {
		return scope;
	}

	public void setScope(Scope[] scope) {
		this.scope = scope;
	}

	public BindingType getType() {
		return type;
	}

	public void setType(BindingType type) {
		this.type = type;
	}
}
