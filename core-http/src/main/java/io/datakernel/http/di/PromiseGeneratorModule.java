package io.datakernel.http.di;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.BindingGenerator;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.promise.Promise;

/**
 * @since 3.0.0
 */
public class PromiseGeneratorModule extends AbstractModule {
	private PromiseGeneratorModule() {
	}

	public static PromiseGeneratorModule create() {
		return new PromiseGeneratorModule();
	}

	@Override
	protected void configure() {
		BindingGenerator<Promise<?>> generator = (bindings, scope, key) -> {
			Binding<Object> binding = bindings.get(key.getTypeParameter(0));
			return binding != null ?
					binding.mapInstance(Promise::of) :
					null;
		};
		generate(Promise.class, generator);
	}
}
