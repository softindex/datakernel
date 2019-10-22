package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.module.BindingType.COMMON;

public final class BindingDesc {
	private Key<?> key;
	private Binding<?> binding;
	private Scope[] scope;
	private BindingType type;
	private boolean exported;

	public BindingDesc(Key<?> key, Binding<?> binding) {
		this.key = key;
		this.binding = binding;
		this.scope = UNSCOPED;
		this.type = COMMON;
		this.exported = false;
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

	public boolean isExported() {
		return exported;
	}

	public void setExported() {
		exported = true;
	}

	public BindingType getType() {
		return type;
	}

	public void setType(BindingType type) {
		this.type = type;
	}
}
