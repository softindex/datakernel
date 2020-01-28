import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.OptionalGeneratorModule;

import java.util.Optional;

/**
 * @since 3.0.0
 */
//[START REGION_1]
public class OptionalGeneratorModuleExample {
	public static void main(String[] args) {
		Injector injector = Injector.of(
				Module.create()
						.install(OptionalGeneratorModule.create())
						.bind(String.class).toInstance("Hello, World")
						.bind(new Key<Optional<String>>() {}));
		Optional<String> instance = injector.getInstance(new Key<Optional<String>>() {});
		System.out.println(instance);
	}
}
//[END REGION_1]
