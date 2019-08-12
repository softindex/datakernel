package io.datakernel;

import io.datakernel.async.Promise;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.http.di.PromiseGeneratorModule;
import org.junit.Test;

import static junit.framework.TestCase.*;

/**
 * @since 3.0.0
 */
public final class PromiseGeneratorTest {
	@Test
	public void promiseGeneratorTest() {
		Module module = Module.create()
				.bind(String.class).to(() -> "Hello, World")
				.bind(new Key<Promise<String>>() {});

		Module module1 = Module.create()
				.install(module)
				.install(PromiseGeneratorModule.create());

		Injector injector = Injector.of(module1);
		Promise<String> instance = injector.getInstance(new Key<Promise<String>>() {});
		assertEquals("Hello, World", instance.getResult());
		assertFalse(injector.hasInstance(new Key<Promise<Integer>>() {}));
	}
}
