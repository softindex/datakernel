package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;

public final class BindingDesc {
	private Key<?> key;
	private Binding<?> binding;
	private Scope[] scope;
	private boolean exported;

	public BindingDesc(Key<?> key, Binding<?> binding, Scope[] scope, boolean exported) {
		this.key = key;
		this.binding = binding;
		this.scope = scope;
		this.exported = exported;
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
}
