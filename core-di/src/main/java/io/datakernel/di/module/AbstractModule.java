package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static io.datakernel.di.util.Utils.checkState;

/**
 * This class is an abstract module wrapper around {@link ModuleBuilder}.
 * It provides functionality that is similar to some other DI frameworks for the ease of transition.
 */
@SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
public abstract class AbstractModule implements Module {
	private Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings;
	private Map<Integer, Set<BindingTransformer<?>>> bindingTransformers;
	private Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators;
	private Map<Key<?>, Multibinder<?>> multibinders;

	private final AtomicBoolean configured = new AtomicBoolean();

	@Nullable
	private ModuleBuilder builder = null;

	@Nullable
	private final StackTraceElement location;

	public AbstractModule() {
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		StackTraceElement found = null;
		Class<?> cls = getClass();
		for (int i = 2; i < trace.length; i++) {
			StackTraceElement element = trace[i];
			try {
				String className = element.getClassName();
				Class<?> traceCls = Class.forName(className);
				if (!traceCls.isAssignableFrom(cls) && !className.startsWith("sun.reflect") && !className.startsWith("java.lang")) {
					found = element;
					break;
				}
			} catch (ClassNotFoundException ignored) {
				break;
			}
		}
		location = found;
	}

	/**
	 * This method is meant to be overridden to call all the <code>bind(...)</code> methods.
	 */
	protected void configure() {
	}

	/**
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilderBinder<T> bind(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(key);
	}

	/**
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilderBinder<T> bind(Class<T> type) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(type);
	}

	/**
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilderBinder<T> bind(Class<T> type, Name name) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(type, name);
	}

	/**
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilderBinder<T> bind(Class<T> type, String name) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(type, name);
	}

	/**
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilderBinder<T> bind(Class<T> type, Class<? extends Annotation> annotationType) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(type, annotationType);
	}

	protected final <T> ModuleBuilder bindInstanceProvider(@NotNull Class<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bindInstanceProvider(key);
	}

	protected final <T> ModuleBuilder bindInstanceProvider(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bindInstanceProvider(key);
	}

	protected final <T> ModuleBuilder bindInstanceInjector(@NotNull Class<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bindInstanceInjector(key);
	}

	protected final <T> ModuleBuilder bindInstanceInjector(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bindInstanceInjector(Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()));
	}

	/**
	 * @see ModuleBuilder#install
	 */
	protected final void install(Module module) {
		checkState(builder != null, "Cannot install modules before or after configure() call");
		builder.install(module);
	}

	/**
	 * @see ModuleBuilder#transform
	 */
	protected final <T> void transform(int priority, BindingTransformer<T> bindingTransformer) {
		checkState(builder != null, "Cannot add transformers before or after configure() call");
		builder.transform(priority, bindingTransformer);
	}

	/**
	 * @see ModuleBuilder#generate
	 */
	protected final <T> void generate(Class<?> pattern, BindingGenerator<T> bindingGenerator) {
		checkState(builder != null, "Cannot add generators before or after configure() call");
		builder.generate(pattern, bindingGenerator);
	}

	/**
	 * @see ModuleBuilder#multibind
	 */
	protected final <T> void multibind(Key<T> key, Multibinder<T> multibinder) {
		checkState(builder != null, "Cannot add multibinders before or after configure() call");
		builder.multibind(key, multibinder);
	}

	protected final <V> void multibindToSet(Class<V> type) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(type);
	}

	protected final <V> void multibindToSet(Class<V> type, String name) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(type, name);
	}

	protected final <V> void multibindToSet(Class<V> type, Name name) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(type, name);
	}

	protected final <V> void multibindToSet(Key<V> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(key);
	}

	protected final <K, V> void multibindToMap(Class<K> keyType, Class<V> valueType) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToMap(keyType, valueType);
	}

	protected final <K, V> void multibindToMap(Class<K> keyType, Class<V> valueType, String name) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToMap(keyType, valueType, name);
	}

	protected final <K, V> void multibindToMap(Class<K> keyType, Class<V> valueType, Name name) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToMap(keyType, valueType, name);
	}

	/**
	 * @see ModuleBuilder#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Binding<T> binding) {
		checkState(builder != null, "Cannot bind into set before or after configure() call");
		builder.bindIntoSet(setOf, binding);
	}

	/**
	 * @see ModuleBuilder#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Key<T> item) {
		bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * @see ModuleBuilder#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(@NotNull Key<S> setOf, @NotNull T element) {
		bindIntoSet(setOf, Binding.toInstance(element));
	}

	protected final void scan(Object object) {
		checkState(builder != null, "Cannot add declarative bindings before or after configure() call");
		builder.scan(object);
	}

	protected final void scan(Class<?> cls) {
		checkState(builder != null, "Cannot add declarative bindings before or after configure() call");
		builder.scan(cls);
	}

	private void finish() {
		if (!configured.compareAndSet(false, true)) {
			return;
		}

		ModuleBuilder b = new ModuleBuilderImpl<>(getName(), location).scan(getClass().getSuperclass(), this);
		ReflectionUtils.scanClassInto(getClass(), this, b); // so that provider methods and dsl bindings are in one 'export area'

		builder = b;
		configure();
		builder = null;

		bindings = b.getBindings();
		bindingTransformers = b.getBindingTransformers();
		bindingGenerators = b.getBindingGenerators();
		multibinders = b.getMultibinders();
	}

	@Override
	public final Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings() {
		finish();
		return bindings;
	}

	@Override
	public final Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		finish();
		return bindingTransformers;
	}

	@Override
	public final Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		finish();
		return bindingGenerators;
	}

	@Override
	public final Map<Key<?>, Multibinder<?>> getMultibinders() {
		finish();
		return multibinders;
	}

	// region forbid overriding default module methods

	@Override
	public Module combineWith(Module another) {
		return Module.super.combineWith(another);
	}

	@Override
	public Module overrideWith(Module another) {
		return Module.super.overrideWith(another);
	}

	@Override
	public Module transformWith(UnaryOperator<Module> fn) {
		return Module.super.transformWith(fn);
	}

	@Override
	public Module export(Key<?> key, Key<?>... keys) {
		return Module.super.export(key, keys);
	}

	@Override
	public Module export(Set<Key<?>> keys) {
		return Module.super.export(keys);
	}

	@Override
	public <V> Module rebindExport(Key<V> from, Key<? extends V> to) {
		return Module.super.rebindExport(from, to);
	}

	@Override
	public <V> Module rebindImport(Key<V> from, Key<? extends V> to) {
		return Module.super.rebindImport(from, to);
	}

	@Override
	public <V> Module rebindImport(Key<V> from, Binding<? extends V> binding) {
		return Module.super.rebindImport(from, binding);
	}

	@Override
	public Module rebindExports(@NotNull Map<Key<?>, Key<?>> mapping) {
		return Module.super.rebindExports(mapping);
	}

	@Override
	public Module rebindImports(@NotNull Map<Key<?>, Binding<?>> mapping) {
		return Module.super.rebindImports(mapping);
	}

	@Override
	public Module rebindImportKeys(@NotNull Map<Key<?>, Key<?>> mapping) {
		return Module.super.rebindImportKeys(mapping);
	}

	@Override
	public <T, V> Module rebindImportDependencies(Key<T> key, Key<V> dependency, Key<? extends V> to) {
		return Module.super.rebindImportDependencies(key, dependency, to);
	}

	@Override
	public <T> Module rebindImportDependencies(Key<T> key, @NotNull Map<Key<?>, Key<?>> dependencyMapping) {
		return Module.super.rebindImportDependencies(key, dependencyMapping);
	}

	@Override
	public Module rebindImports(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return Module.super.rebindImports(rebinder);
	}

	// endregion

	private String getName() {
		Class<?> cls = getClass();
		return ReflectionUtils.getDisplayName(cls.isAnonymousClass() ? cls.getGenericSuperclass() : cls);
	}

	@Override
	public String toString() {
		return getName() + "(at " + (location != null ? location : "<unknown module location>") + ')';
	}
}
