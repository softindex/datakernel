package io.datakernel.test;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.test.rules.LambdaStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;
import java.util.*;

import static io.datakernel.common.collection.CollectionUtils.union;
import static java.util.stream.Collectors.toSet;

public class DatakernelRunner extends BlockJUnit4ClassRunner {
	private final Set<FrameworkMethod> surroundings = new HashSet<>();
	private final Set<Dependency> staticDependencies;

	private Module currentModule;
	private Set<Dependency> currentDependencies;

	protected Injector currentInjector;

	public DatakernelRunner(Class<?> cls) throws InitializationError {
		super(cls);

		surroundings.addAll(getTestClass().getAnnotatedMethods(Before.class));
		surroundings.addAll(getTestClass().getAnnotatedMethods(After.class));

		staticDependencies = surroundings.stream()
				.flatMap(m -> Arrays.stream(ReflectionUtils.toDependencies(cls, m.getMethod().getParameters())))
				.collect(toSet());
	}

	// runChild is always called before createTest
	@Override
	protected void runChild(FrameworkMethod method, RunNotifier notifier) {
		Description description = describeChild(method);
		if (isIgnored(method)) {
			notifier.fireTestIgnored(description);
			return;
		}
		try {
			Class<?> cls = getTestClass().getJavaClass();

			Set<Module> modules = new HashSet<>();

			addClassModules(modules, cls); // add modules from class annotation
			addMethodModules(modules, method); // add modules from current test method
			for (FrameworkMethod m : surroundings) { // add modules from befores and afters
				addMethodModules(modules, m);
			}
			currentModule = Modules.combine(modules);

			currentDependencies =
					Arrays.stream(ReflectionUtils.toDependencies(cls, method.getMethod().getParameters()))
							.collect(toSet());

		} catch (ExceptionInInitializerError e) {
			Throwable cause = e.getCause();
			notifier.fireTestFailure(new Failure(description, cause != null ? cause : e));
			return;
		} catch (Exception e) {
			notifier.fireTestFailure(new Failure(description, e));
			return;
		}

		runLeaf(methodBlock(method), description, notifier);
	}

	private static void addClassModules(Set<Module> modules, Class<?> cls) throws IllegalAccessException, InstantiationException, ExceptionInInitializerError {
		while (cls != null) {
			UseModules useModules = cls.getAnnotation(UseModules.class);
			if (useModules != null) {
				for (Class<? extends Module> moduleClass : useModules.value()) {
					modules.add(moduleClass.newInstance());
				}
			}
			cls = cls.getSuperclass();
		}
	}

	private static void addMethodModules(Set<Module> modules, FrameworkMethod method) throws IllegalAccessException, InstantiationException, ExceptionInInitializerError {
		UseModules useModules = method.getMethod().getAnnotation(UseModules.class);
		if (useModules == null) {
			return;
		}
		for (Class<? extends Module> moduleClass : useModules.value()) {
			modules.add(moduleClass.newInstance());
		}
	}

	private static final class DependencyToken {}

	// createTest is always called after runChild
	@Override
	protected Object createTest() throws Exception {
		Object instance = super.createTest();

		Key<Object> self = Key.ofType(getTestClass().getJavaClass());

		currentInjector = Injector.of(currentModule, Module.create()
				.scan(instance)

				// bind unusable private type and add all the extra dependencies to it so that injector knows about them
				.bind(DependencyToken.class).to(Binding.<DependencyToken>to(() -> {
					throw new AssertionError("should never be instantiated");
				}).addDependencies(union(currentDependencies, staticDependencies)))

				// bind test class to existing instance, and make it eager for the postInject
				.bind(self).toInstance(instance).asEagerSingleton()
				// and make it post-injectable
				.postInjectInto(self));

		currentInjector.createEagerSingletons();
		currentInjector.postInjectInstances();
		return instance;
	}

	// allow test methods to have any arguments
	@Override
	protected void validatePublicVoidNoArgMethods(Class<? extends Annotation> annotation, boolean isStatic, List<Throwable> errors) {
		for (FrameworkMethod testMethod : getTestClass().getAnnotatedMethods(annotation)) {
			testMethod.validatePublicVoid(isStatic, errors);
		}
	}

	// invoke methods with args fetched from current injector
	@Override
	protected Statement methodInvoker(FrameworkMethod method, Object test) {
		return new LambdaStatement(() -> method.invokeExplosively(test, getArgs(method)));
	}

	protected Object[] getArgs(FrameworkMethod method) {
		return Arrays.stream(ReflectionUtils.toDependencies(getTestClass().getJavaClass(), method.getMethod().getParameters()))
				.map(dependency -> dependency.isRequired() ?
						currentInjector.getInstance(dependency.getKey()) :
						currentInjector.getInstanceOrNull(dependency.getKey()))
				.toArray(Object[]::new);
	}

	// same as original except that methods are called like in methodInvoker method
	@Override
	protected Statement withBefores(FrameworkMethod method, Object target, Statement test) {
		List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(Before.class);
		if (methods.isEmpty()) {
			return test;
		}
		return new LambdaStatement(() -> {
			for (FrameworkMethod m : methods) {
				m.invokeExplosively(target, getArgs(m));
			}
			test.evaluate();
		});
	}

	// same as above
	@Override
	protected Statement withAfters(FrameworkMethod method, Object target, Statement test) {
		List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(After.class);
		if (methods.isEmpty()) {
			return test;
		}
		return new LambdaStatement(() -> {
			List<Throwable> errors = new ArrayList<>();
			try {
				test.evaluate();
			} catch (Throwable e) {
				errors.add(e);
			} finally {
				for (FrameworkMethod m : methods) {
					try {
						m.invokeExplosively(target, getArgs(m));
					} catch (Throwable e) {
						errors.add(e);
					}
				}
			}
			MultipleFailureException.assertEmpty(errors);
		});
	}
}
