package io.datakernel.test;

import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceInjector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
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

import java.lang.annotation.*;
import java.util.*;

import static io.datakernel.di.util.ReflectionUtils.keyOf;
import static io.datakernel.di.util.Types.parameterized;
import static io.datakernel.util.CollectionUtils.union;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;

public class DatakernelRunner extends BlockJUnit4ClassRunner {

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.TYPE, ElementType.METHOD})
	public @interface UseModules {
		Class<? extends Module>[] value();
	}

	private final List<FrameworkMethod> beforesAndAfters = new ArrayList<>();
	private final Set<Dependency> staticDependencies;

	protected Injector currentInjector;
	private Module currentModule;
	private Set<Dependency> currentDependencies;

	public DatakernelRunner(Class<?> clazz) throws InitializationError {
		super(clazz);

		beforesAndAfters.addAll(getTestClass().getAnnotatedMethods(Before.class));
		beforesAndAfters.addAll(getTestClass().getAnnotatedMethods(After.class));

		staticDependencies = beforesAndAfters.stream()
				.flatMap(m -> Arrays.stream(ReflectionUtils.toDependencies(clazz, m.getMethod().getParameters())))
				.collect(toSet());
	}

	// allow test methods to have any arguments
	@Override
	protected void validatePublicVoidNoArgMethods(Class<? extends Annotation> annotation, boolean isStatic, List<Throwable> errors) {
		for (FrameworkMethod testMethod : getTestClass().getAnnotatedMethods(annotation)) {
			testMethod.validatePublicVoid(isStatic, errors);
		}
	}

	@Override
	protected Statement methodInvoker(FrameworkMethod method, Object test) {
		return new LambdaStatement(() -> {
			Object[] args = Arrays.stream(method.getMethod().getParameters())
					.map(parameter -> currentInjector.getInstance(keyOf(getTestClass().getJavaClass(), parameter.getParameterizedType(), parameter)))
					.toArray(Object[]::new);

			method.invokeExplosively(test, args);
		});
	}

	@Override
	protected Statement withBefores(FrameworkMethod method, Object target, Statement test) {
		Statement before = createCleanupStatement(target, Before.class);
		return new LambdaStatement(() -> {
			before.evaluate();
			test.evaluate();
		});
	}

	@Override
	protected Statement withAfters(FrameworkMethod method, Object target, Statement test) {
		Statement after = createCleanupStatement(target, After.class);
		return new LambdaStatement(() -> {
			List<Throwable> errors = new ArrayList<>();
			try {
				test.evaluate();
			} catch (Throwable e) {
				errors.add(e);
			} finally {
				try {
					after.evaluate();
				} catch (Throwable e) {
					errors.add(e);
				}
			}
			MultipleFailureException.assertEmpty(errors);
		});
	}

	private Statement createCleanupStatement(Object target, Class<? extends Annotation> annotation) {
		List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(annotation);
		if (methods.isEmpty()) {
			return LambdaStatement.EMPTY;
		}
		Set<Statement> statements = methods.stream()
				.map(m -> methodInvoker(m, target))
				.collect(toSet());
		return new LambdaStatement(() -> {
			for (Statement statement : statements) {
				statement.evaluate();
			}
		});
	}

	@Override
	protected Object createTest() throws Exception {
		Object instance = super.createTest();

		Key<Object> self = Key.ofType(getTestClass().getJavaClass());
		Key<InstanceInjector<Object>> injectorKey = Key.ofType(parameterized(InstanceInjector.class, self.getType()));

		currentInjector = Injector.of(currentModule, new AbstractModule() {{
			addDeclarativeBindingsFrom(instance);

			// have all the dependencies from methods in binding for the test class instance
			bind(self).to($ -> instance, union(currentDependencies, staticDependencies).toArray(new Dependency[0]));

			bind(injectorKey);
		}});
		currentInjector.getInstance(injectorKey).inject(instance);
		return currentInjector.getInstance(self); // eagerly trigger all the dependencies for fail-fast testing
	}

	@Override
	protected void runChild(FrameworkMethod method, RunNotifier notifier) {
		Description description = describeChild(method);
		if (isIgnored(method)) {
			notifier.fireTestIgnored(description);
			return;
		}

		try {
			Set<Module> modules = new HashSet<>();

			addClassModules(modules);
			addMethodModules(modules, method);
			for (FrameworkMethod m : beforesAndAfters) {
				addMethodModules(modules, m);
			}
			currentModule = Modules.combine(modules);
			currentDependencies = new HashSet<>(asList(ReflectionUtils.toDependencies(getTestClass().getJavaClass(), method.getMethod().getParameters())));
		} catch (Exception e) {
			notifier.fireTestFailure(new Failure(description, e));
			return;
		}

		runLeaf(methodBlock(method), description, notifier);
	}

	private void addClassModules(Set<Module> modules) throws IllegalAccessException, InstantiationException {
		Class<?> currentClass = getTestClass().getJavaClass();
		while (currentClass != null) {
			UseModules useModules = currentClass.getAnnotation(UseModules.class);
			if (useModules != null) {
				for (Class<? extends Module> moduleClass : useModules.value()) {
					modules.add(moduleClass.newInstance());
				}
			}
			currentClass = currentClass.getSuperclass();
		}
	}

	private void addMethodModules(Set<Module> modules, FrameworkMethod method) throws IllegalAccessException, InstantiationException {
		UseModules useModules = method.getMethod().getAnnotation(UseModules.class);
		if (useModules == null) {
			return;
		}
		for (Class<? extends Module> moduleClass : useModules.value()) {
			modules.add(moduleClass.newInstance());
		}
	}
}
