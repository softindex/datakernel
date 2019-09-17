package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static io.datakernel.di.util.Utils.checkState;

/**
 * This class is an abstract module wrapper around {@link BuilderModule}.
 * It provides functionality that is similar to some other DI frameworks for the ease of transition.
 */
public abstract class AbstractModule implements Module {
	private Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings;
	private Map<Integer, Set<BindingTransformer<?>>> bindingTransformers;
	private Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators;
	private Map<Key<?>, Multibinder<?>> multibinders;

	private AtomicBoolean configured = new AtomicBoolean();

	@Nullable
	private BuilderModuleBindingStage builder = null;

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
	 * @see BuilderModule#bind(Key)
	 */
	protected final <T> BuilderModule<T> bind(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");

		// support generics from subclasses of this class
		return builder.bind(Key.ofType(Types.resolveTypeVariables(key.getType(), getClass()), key.getName()));
	}

	/**
	 * @see BuilderModule#bind(Key)
	 */
	protected final <T> BuilderModule<T> bind(Class<T> type) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(Key.of(type));
	}

	/**
	 * @see BuilderModule#install
	 */
	protected final void install(Module module) {
		checkState(builder != null, "Cannot install modules before or after configure() call");
		builder.install(module);
	}

	/**
	 * @see BuilderModule#transform
	 */
	protected final <T> void transform(int priority, BindingTransformer<T> bindingTransformer) {
		checkState(builder != null, "Cannot add transformers before or after configure() call");
		builder.transform(priority, bindingTransformer);
	}

	/**
	 * @see BuilderModule#generate
	 */
	protected final <T> void generate(Class<?> pattern, BindingGenerator<T> bindingGenerator) {
		checkState(builder != null, "Cannot add generators before or after configure() call");
		builder.generate(pattern, bindingGenerator);
	}

	/**
	 * @see BuilderModule#multibind
	 */
	protected final <T> void multibind(Key<T> key, Multibinder<T> multibinder) {
		checkState(builder != null, "Cannot add multibinders before or after configure() call");
		builder.multibind(key, multibinder);
	}

	/**
	 * @see BuilderModule#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Binding<T> binding) {
		checkState(builder != null, "Cannot bind into set before or after configure() call");
		builder.bindIntoSet(setOf, binding);
	}

	/**
	 * @see BuilderModule#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Key<T> item) {
		bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * @see BuilderModule#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(@NotNull Key<S> setOf, @NotNull T element) {
		bindIntoSet(setOf, Binding.toInstance(element));
	}

	/**
	 * @see BuilderModule#postInjectInto(Key)
	 */
	protected final <T> void postInjectInto(Key<T> key) {
		checkState(builder != null, "Cannot post inject into something before or after configure() call");
		builder.postInjectInto(key);
	}

	/**
	 * @see BuilderModule#postInjectInto(Key)
	 */
	protected final <T> void postInjectInto(Class<T> type) {
		postInjectInto(Key.of(type));
	}

	protected final void scan(Object object) {
		checkState(builder != null, "Cannot add declarative bindings before or after configure() call");
		builder.scan(object);
	}

	protected final void scan(Class<?> cls) {
		checkState(builder != null, "Cannot add declarative bindings before or after configure() call");
		builder.scanStatics(cls);
	}

	private void finish() {
		if (!configured.compareAndSet(false, true)) {
			return;
		}

		BuilderModuleBindingStage b = Module.create().scan(this);
		builder = b;
		configure();
		builder = null;

		bindings = b.getBindings();
		bindingTransformers = b.getBindingTransformers();
		bindingGenerators = b.getBindingGenerators();
		multibinders = b.getMultibinders();
	}

	@Override
	public final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
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
	public final Module combineWith(Module another) {
		return Module.super.combineWith(another);
	}

	@Override
	public final Module overrideWith(Module another) {
		return Module.super.overrideWith(another);
	}

	@Override
	public final Module transformWith(UnaryOperator<Module> fn) {
		return Module.super.transformWith(fn);
	}

	@Override
	public final Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return Module.super.resolveBindings();
	}

	@Override
	public <V> Module rebindExport(Key<V> from, Key<? extends V> to) {
		return Module.super.rebindExport(from, to);
	}

	@Override
	public Module rebindExports(@NotNull Map<Key<?>, Key<?>> map) {
		return Module.super.rebindExports(map);
	}

	@Override
	public <V> Module rebindImport(Key<V> from, Key<? extends V> to) {
		return Module.super.rebindImport(from, to);
	}

	@Override
	public Module rebindImports(@NotNull Map<Key<?>, Key<?>> map) {
		return Module.super.rebindImports(map);
	}

	@Override
	public <T, V> Module rebindImports(Key<T> componentKey, Key<V> from, Key<? extends V> to) {
		return Module.super.rebindImport(from, to);
	}

	@Override
	public <T> Module rebindImports(Key<T> componentKey, @NotNull Map<Key<?>, Key<?>> map) {
		return Module.super.rebindImports(componentKey, map);
	}

	@Override
	public Module rebindImports(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return Module.super.rebindImports(rebinder);
	}

	@Override
	public final Module export(Key<?>... keys) {
		return Module.super.export(keys);
	}

	@Override
	public final Module export(Set<Key<?>> keys) {
		return Module.super.export(keys);
	}
	// endregion

	@Override
	public String toString() {
		Class<?> cls = getClass();
		return ReflectionUtils.getShortName(cls.isAnonymousClass() ? cls.getGenericSuperclass() : cls) +
				"(at " + (location != null ? location : "<unknown module location>") + ')';
	}
}
