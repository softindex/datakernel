package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.BindingGenerator;

import java.util.Optional;

/**
 * @since 3.0.0
 */
public class OptionalGeneratorModule extends AbstractModule {

	private OptionalGeneratorModule() {
	}

	public static OptionalGeneratorModule create() {
		return new OptionalGeneratorModule();
	}

	@Override
	protected void configure() {
		BindingGenerator<?> optionalGenerator = (bindings, scope, key) -> {
			Binding<Object> binding = bindings.get(key.getTypeParameter(0));
			return binding != null ?
					binding.mapInstance(Optional::of) :
					Binding.toInstance(Optional.empty());
		};

		generate(Optional.class, optionalGenerator);
	}
}
