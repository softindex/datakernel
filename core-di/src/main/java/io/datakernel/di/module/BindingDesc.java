package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;

import static io.datakernel.di.core.Scope.UNSCOPED;

public final class BindingDesc {
	private Key<?> key;
	private Binding<?> binding;
	private Scope[] scope;
	private boolean exported;
	private boolean isTransient;

	public BindingDesc(Key<?> key, Binding<?> binding) {
		this.key = key;
		this.binding = binding;
		this.scope = UNSCOPED;
		this.exported = false;
		this.isTransient = false;
	}

	public void setKey(Key<?> key) {
		this.key = key;
	}

	public void setBinding(Binding<?> binding) {
		this.binding = binding;
	}

	public void setScope(Scope[] scope) {
		this.scope = scope;
	}

	public void setExported() {
		exported = true;
	}

	public void setTransient() {
		isTransient = true;
	}

	public Key<?> getKey() {
		return key;
	}

	public Binding<?> getBinding() {
		return binding;
	}

	public Scope[] getScope() {
		return scope;
	}

	public boolean isExported() {
		return exported;
	}

	public boolean isTransient() {
		return isTransient;
	}
}
