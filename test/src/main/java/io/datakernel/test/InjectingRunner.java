package io.datakernel.test;

import io.datakernel.di.Injector;
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

public final class InjectingRunner extends BlockJUnit4ClassRunner {

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface UseModules {
		Class<? extends Module>[] value();
	}

	@Nullable
	private final Module module;

	private final Class<?> testClass;

	public InjectingRunner(Class<?> testClass) throws InitializationError, ReflectiveOperationException {
		super(testClass);
		this.testClass = testClass;

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

	@SuppressWarnings("unchecked")
	@Override
	protected Object createTest() throws Exception {
		Object instance = super.createTest();
		if (module == null) {
			return instance;
		}
		Injector injector = Injector.of(module, new AbstractModule() {{
			bind((Class<Object>) testClass).toInstance(instance);

			addDeclarativeBindingsFrom(instance);
		}});
		return injector.getInstance(testClass);
	}
}
