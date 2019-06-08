package io.datakernel.di.error;

import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Scope;

public final class NoBindingsInScopeException extends IllegalArgumentException {
	private final Injector injector;
	private final Scope scope;

	public NoBindingsInScopeException(Injector injector, Scope scope) {
		super("Tried to enter a scope " + scope + " that was not represented by any binding");
		this.injector = injector;
		this.scope = scope;
	}

	public Injector getInjector() {
		return injector;
	}

	public Scope getScope() {
		return scope;
	}
}
