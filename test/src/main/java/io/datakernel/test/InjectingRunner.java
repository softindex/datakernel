package io.datakernel.test;

import io.datakernel.di.Injector;
import io.datakernel.di.InstanceInjector;
import io.datakernel.di.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import org.jetbrains.annotations.Nullable;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;

import static io.datakernel.di.util.ReflectionUtils.parameterized;

public final class InjectingRunner extends BlockJUnit4ClassRunner {

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface UseModules {
		Class<? extends Module>[] value();
	}

	@Nullable
	private final Module module;


	public InjectingRunner(Class<?> testClass) throws InitializationError, ReflectiveOperationException {
		super(testClass);

		UseModules annotation = testClass.getAnnotation(UseModules.class);
		if (annotation == null) {
			module = null;
			return;
		}

		Class<?>[] classes = annotation.value();

		if (classes.length == 0) {
			module = null;
			return;
		}

		Module[] modules = new Module[classes.length];

		for (int i = 0; i < classes.length; i++) {
			Class<?> moduleClass = classes[i];
			Constructor<?> constructor = moduleClass.getDeclaredConstructor();
			constructor.setAccessible(true);
			modules[i] = (Module) constructor.newInstance();
		}

		module = Modules.combine(modules);
	}

	@Override
	protected Object createTest() throws Exception {
		Object instance = super.createTest();
		if (module == null) {
			return instance;
		}
		Key<InstanceInjector<Object>> key = Key.ofType(parameterized(InstanceInjector.class, instance.getClass()));
		Injector injector = Injector.of(module, new AbstractModule() {{
			bind(key);
			addDeclarativeBindingsFrom(instance);
		}});
		injector.getInstance(key).inject(instance);

		return instance;
	}
}
