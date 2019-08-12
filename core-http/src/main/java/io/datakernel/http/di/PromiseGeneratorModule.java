package io.datakernel.http.di;

import io.datakernel.async.Promise;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.BindingGenerator;
import io.datakernel.di.module.AbstractModule;

/**
 * @since 3.0.0
 */
public class PromiseGeneratorModule extends AbstractModule {
	public static PromiseGeneratorModule create() {
		return new PromiseGeneratorModule();
	}

	private PromiseGeneratorModule() {
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
