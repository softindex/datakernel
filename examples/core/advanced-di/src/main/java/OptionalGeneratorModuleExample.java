import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.OptionalGeneratorModule;

import java.util.Optional;

/**
 * @since 3.0.0
 */
//[START REGION_1]
public class OptionalGeneratorModuleExample extends AbstractModule {
	@Inject
	Optional<String> string;

	public static void main(String[] args) {
		Module module = Module.create()
				.install(new OptionalGeneratorModuleExample())
				.install(OptionalGeneratorModule.create())
				.bind(String.class).toInstance("Hello, World")
				.bind(new Key<Optional<String>>() {});

		Injector injector = Injector.of(module);
		Optional<String> instance = injector.getInstance(new Key<Optional<String>>() {});
		System.out.println(instance.orElseThrow(
				() -> new IllegalStateException("Param is not found")));
	}
}
//[END REGION_1]
