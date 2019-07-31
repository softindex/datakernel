package io.datakernel.di.module;

import io.datakernel.di.annotation.EagerSingleton;
import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.ReflectionUtils.ProviderScanResults;
import static io.datakernel.di.util.ReflectionUtils.scanProviderMethods;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("UnusedReturnValue")
public final class BuilderModule<T> implements BuilderModuleBindingStage {
	private final List<BindingDesc> bindingDescs = new ArrayList<>();

	private Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());
	private Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	private AtomicBoolean configured = new AtomicBoolean();

	@Nullable
	private volatile BindingDesc current = null;

	BuilderModule() {
	}

	private void completeCurrent() {
		BindingDesc prev = current;
		if (prev != null) {
			bindingDescs.add(prev);
			current = null;
		}
	}

	/**
	 * This method begins a chain of binding builder DSL calls.
	 * <p>
	 * You can use generics in it, only those that are defined at the module class.
	 * And you need to subclass the module at the usage point to 'bake' those generics
	 * into subclass bytecode so that they could be fetched by this bind call.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <U> BuilderModule<U> bind(@NotNull Key<U> key) {
		checkState(!configured.get(), "Cannot bind after the module builder was used as a module");
		completeCurrent();
		current = new BindingDesc(key, new Binding<>(emptySet(), Preprocessor.TO_BE_GENERATED), UNSCOPED, false);
		return (BuilderModule<U>) this;
	}

	/**
	 * @see #bind(Key)
	 */
	@Override
	public <U> BuilderModule<U> bind(Class<U> type) {
		return bind(Key.of(type));
	}

	private BindingDesc ensureCurrent() {
		checkState(!configured.get(), "Cannot use the module builder DSL after the module was used");
		BindingDesc desc = current;
		checkState(desc != null, "Cannot configure binding before bind(...) call");
		return desc;
	}

	/**
	 * If bound key does not have a name already then sets it to a given one
	 */
	public BuilderModule<T> annotatedWith(@NotNull Name name) {
		BindingDesc desc = ensureCurrent();
		Key<?> key = desc.getKey();
		if (key.getName() != null) {
			throw new IllegalStateException("Already annotated with " + key.getName().getDisplayString());
		}
		desc.setKey(key.named(name));
		return this;
	}

	/**
	 * @see #annotatedWith(Name)
	 */
	public BuilderModule<T> annotatedWith(@NotNull Annotation annotation) {
		return annotatedWith(Name.of(annotation));
	}

	/**
	 * @see #annotatedWith(Name)
	 */
	public BuilderModule<T> annotatedWith(@NotNull Class<? extends Annotation> annotationType) {
		return annotatedWith(Name.of(annotationType));
	}

	/**
	 * The binding being built by this builder will be added to the binding graph trie at given scope path
	 */
	public BuilderModule<T> in(@NotNull Scope[] scope) {
		BindingDesc desc = ensureCurrent();
		if (desc.getScope().length != 0) {
			throw new IllegalStateException("Already bound to scope " + getScopeDisplayString(desc.getScope()));
		}
		desc.setScope(scope);
		return this;
	}

	/**
	 * @see #in(Scope[])
	 */
	public BuilderModule<T> in(@NotNull Scope scope, @NotNull Scope... scopes) {
		Scope[] joined = new Scope[scopes.length + 1];
		joined[0] = scope;
		System.arraycopy(scopes, 0, joined, 1, scopes.length);
		return in(joined);
	}

	/**
	 * @see #in(Scope[])
	 */
	@SafeVarargs
	public final BuilderModule<T> in(@NotNull Class<? extends Annotation> annotationClass, @NotNull Class<? extends Annotation>... annotationClasses) {
		return in(Stream.concat(Stream.of(annotationClass), Arrays.stream(annotationClasses)).map(Scope::of).toArray(Scope[]::new));
	}

	/**
	 * Sets a binding which would be bound to a given key and added to the binding graph trie
	 */
	public BuilderModule<T> to(@NotNull Binding<? extends T> binding) {
		BindingDesc desc = ensureCurrent();
		if (desc.getBinding().getCompiler() != Preprocessor.TO_BE_GENERATED) {
			throw new IllegalStateException("Already mapped to a binding");
		}
		if (binding.getLocation() == null) {
			binding.at(LocationInfo.from(this));
		}
		desc.setBinding(binding);
		return this;
	}

	/**
	 * DSL shortcut for creating a binding that just calls a binding at given key
	 * and {@link #to(Binding) binding it} to current key.
	 */
	public BuilderModule<T> to(@NotNull Key<? extends T> implementation) {
		return to(Binding.to(implementation));
	}

	/**
	 * @see #to(Key)
	 */
	public BuilderModule<T> to(@NotNull Class<? extends T> implementation) {
		return to(Binding.to(implementation));
	}

	/**
	 * DSL shortcut for creating a binding from a given instance
	 * and {@link #to(Binding) binding it} to current key.
	 */
	public <U extends T> BuilderModule<T> toInstance(@NotNull U instance) {
		return to(Binding.toInstance(instance));
	}

	/**
	 * DSL shortcut for creating a dummy binding that will not create any instances
	 * because they will be added later dynamically by calling {@link Injector#putInstance}.
	 */
	public BuilderModule<T> toDynamic() {
		BindingDesc desc = ensureCurrent();
		Key<?> key = desc.getKey();
		Scope[] scope = desc.getScope();
		desc.setBinding(Binding.to(() -> {
			throw new AssertionError("No instance was put into the injector dynamically for key " + key.getDisplayString() + " in scope " + getScopeDisplayString(scope));
		}));
		return this;
	}

	/**
	 * DSL shortcut for creating a binding that calls a supplier from binding at given key
	 * and {@link #to(Binding) binding it} to current key.
	 */
	public BuilderModule<T> toSupplier(@NotNull Key<? extends Supplier<? extends T>> supplierKey) {
		return to(Binding.toSupplier(supplierKey));
	}

	/**
	 * @see #toSupplier(Key)
	 */
	public BuilderModule<T> toSupplier(@NotNull Class<? extends Supplier<? extends T>> supplierType) {
		return to(Binding.toSupplier(supplierType));
	}

	// region public BuilderModule<T> to(constructor*, dependencies...) { ... }

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public BuilderModule<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Class<?>[] dependencies) {
		return to(Binding.to(factory, dependencies));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public BuilderModule<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Key<?>[] dependencies) {
		return to(Binding.to(factory, dependencies));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public BuilderModule<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Dependency[] dependencies) {
		return to(Binding.to(factory, dependencies));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public BuilderModule<T> to(@NotNull Constructor0<? extends T> constructor) {
		return to(Binding.to(constructor));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1> BuilderModule<T> to(@NotNull Constructor1<T1, ? extends T> constructor,
			@NotNull Class<T1> dependency1) {
		return to(Binding.to(constructor, dependency1));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2> BuilderModule<T> to(@NotNull Constructor2<T1, T2, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2) {
		return to(Binding.to(constructor, dependency1, dependency2));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3> BuilderModule<T> to(@NotNull Constructor3<T1, T2, T3, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3, T4> BuilderModule<T> to(@NotNull Constructor4<T1, T2, T3, T4, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3, T4, T5> BuilderModule<T> to(@NotNull Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3, T4, T5, T6> BuilderModule<T> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5, @NotNull Class<T6> dependency6) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1> BuilderModule<T> to(@NotNull Constructor1<T1, ? extends T> constructor,
			@NotNull Key<T1> dependency1) {
		return to(Binding.to(constructor, dependency1));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2> BuilderModule<T> to(@NotNull Constructor2<T1, T2, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
		return to(Binding.to(constructor, dependency1, dependency2));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3> BuilderModule<T> to(@NotNull Constructor3<T1, T2, T3, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3, T4> BuilderModule<T> to(@NotNull Constructor4<T1, T2, T3, T4, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3, T4, T5> BuilderModule<T> to(@NotNull Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	public <T1, T2, T3, T4, T5, T6> BuilderModule<T> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
	}

	// endregion

	/**
	 * Adds bound key to a key set named with given name.
	 * <p>
	 * A key set is a special key of type Set&lt;Key&lt;?&gt;&gt; and name with annotation that is
	 * marked with {@link KeySetAnnotation} instead of {@link NameAnnotation}
	 *
	 * @see KeySetAnnotation
	 */
	public BuilderModule<T> as(@NotNull Name name) {
		checkArgument(name.isMarkedBy(KeySetAnnotation.class), "Should be a key set name");
		checkState(!configured.get(), "Cannot use the module builder DSL after the module was used");

		Key<Set<Key<?>>> setKey = new Key<Set<Key<?>>>(name) {};

		BindingDesc desc = ensureCurrent();

		// binding constructor closes over the desc because the key could be modified after the .as() call
		bindingDescs.add(new BindingDesc(setKey, Binding.to(() -> singleton(desc.getKey())), UNSCOPED, false));

		multibinders.put(setKey, Multibinder.toSet());
		return this;
	}

	/**
	 * @see #as(Name)
	 */
	public BuilderModule<T> as(@NotNull Class<? extends Annotation> annotationType) {
		return as(Name.of(annotationType));
	}

	/**
	 * @see #as(Name)
	 */
	public BuilderModule<T> as(@NotNull Annotation annotation) {
		return as(Name.of(annotation));
	}

	/**
	 * @see #as(Name)
	 */
	public BuilderModule<T> as(@NotNull String name) {
		return as(Name.of(name));
	}

	/**
	 * A shortcut for <code>as({@link EagerSingleton}.class)</code>
	 */
	public BuilderModule<T> asEagerSingleton() {
		return as(EagerSingleton.class);
	}

	/**
	 * Adds given dependencies to the underlying binding
	 */
	public BuilderModule<T> withExtraDependencies(Dependency... dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	/**
	 * @see #withExtraDependencies(Dependency...)
	 */
	public BuilderModule<T> withExtraDependencies(Key<?>... dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	/**
	 * @see #withExtraDependencies(Dependency...)
	 */
	public BuilderModule<T> withExtraDependencies(Class<?>... dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	public BuilderModule<T> export() {
		BindingDesc current = ensureCurrent();
		checkState(!current.isExported(), "Binding was already exported");
		current.setExported();
		return this;
	}

	private BuilderModuleBindingStage scan(@NotNull Class<?> moduleClass, @Nullable Object module) {
		checkState(!configured.get(), "Cannot add declarative bindings after the module builder was used as a module");
		completeCurrent();
		ProviderScanResults results = scanProviderMethods(moduleClass, module);
		bindingDescs.addAll(results.getBindingDescs());
		multibinders.putAll(results.getMultibinders());
		combineMultimap(bindingGenerators, results.getBindingGenerators());
		return this;
	}

	@Override
	public BuilderModuleBindingStage scan(Object container) {
		return scan(container.getClass(), container);
	}

	@Override
	public BuilderModuleBindingStage scanStatics(Class<?> container) {
		return scan(container, null);
	}

	/**
	 * This method simply adds all bindings, transformers, generators and multibinders from given modules to this one.
	 */
	@Override
	public BuilderModuleBindingStage install(Collection<Module> modules) {
		checkState(!configured.get(), "Cannot install modules after the module builder was used as a module");
		completeCurrent();
		for (Module module : modules) {
			bindings.addAll(module.getBindings(), multimapMerger());
			combineMultimap(bindingTransformers, module.getBindingTransformers());
			combineMultimap(bindingGenerators, module.getBindingGenerators());
			mergeMultibinders(multibinders, module.getMultibinders());
		}
		return this;
	}

	/**
	 * @see #install(Collection)
	 */
	@Override
	public BuilderModuleBindingStage install(Module... modules) {
		return install(Arrays.asList(modules));
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinder#toSet multibinds} each of such sets together.
	 */
	@Override
	public <S, E extends S> BuilderModuleBindingStage bindIntoSet(Key<S> setOf, Binding<E> binding) {
		checkState(!configured.get(), "Cannot install modules after the module builder was used as a module");

		completeCurrent();

		Key<Set<S>> set = Key.ofType(Types.parameterized(Set.class, setOf.getType()), setOf.getName());

		bindingDescs.add(new BindingDesc(set, binding.mapInstance(Collections::singleton), UNSCOPED, false));

		multibinders.put(set, Multibinder.toSet());
		return this;
	}

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#to(Key)
	 */
	@Override
	public <S, E extends S> BuilderModuleBindingStage bindIntoSet(Key<S> setOf, Key<E> item) {
		return bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#toInstance(Object)
	 */
	@Override
	public <S, E extends S> BuilderModuleBindingStage bindIntoSet(@NotNull Key<S> setOf, @NotNull E element) {
		return bindIntoSet(setOf, Binding.toInstance(element));
	}

	/**
	 * {@link #bindIntoSet(Key, Key) Binds into set} a key of instance injector for given type at a {@link Injector#postInjectInstances special}
	 * key Set&lt;InstanceInjector&lt;?&gt;&gt;.
	 * <p>
	 * Instance injector bindings are {@link DefaultModule generated automatically}.
	 *
	 * @see Injector#postInjectInstances
	 */
	@Override
	public BuilderModuleBindingStage postInjectInto(Key<?> key) {
		return bindIntoSet(new Key<InstanceInjector<?>>() {}, Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()));
	}

	/**
	 * @see #postInjectInto(Key)
	 */
	@Override
	public BuilderModuleBindingStage postInjectInto(Class<?> type) {
		return postInjectInto(Key.of(type));
	}

	/**
	 * Adds a {@link BindingTransformer transformer} with a given priority to this module.
	 */
	@Override
	public <E> BuilderModuleBindingStage transform(int priority, BindingTransformer<E> bindingTransformer) {
		checkState(!configured.get(), "Cannot add transformers after the module builder was used as a module");
		completeCurrent();

		bindingTransformers.computeIfAbsent(priority, $ -> new HashSet<>()).add(bindingTransformer);
		return this;
	}

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 */
	@Override
	public <E> BuilderModuleBindingStage generate(Class<?> pattern, BindingGenerator<E> bindingGenerator) {
		checkState(!configured.get(), "Cannot add generators after the module builder was used as a module");
		completeCurrent();

		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>()).add(bindingGenerator);

		return this;
	}

	/**
	 * Adds a {@link Multibinder multibinder} for a given key to this module.
	 */
	@Override
	public <E> BuilderModuleBindingStage multibind(Key<E> key, Multibinder<E> multibinder) {
		checkState(!configured.get(), "Cannot add multibinders after the module builder was used as a module");
		completeCurrent();

		multibinders.put(key, multibinder);
		return this;
	}

	private void finish() {
		if (!configured.compareAndSet(false, true)) {
			return;
		}
		completeCurrent(); // finish the last binding

		bindingDescs.forEach(b ->
				bindings.computeIfAbsent(b.getScope(), $ -> new HashMap<>())
						.get()
						.computeIfAbsent(b.getKey(), $ -> new HashSet<>())
						.add(b.getBinding()));

		Set<Key<?>> exportedKeys = bindingDescs.stream()
				.filter(BindingDesc::isExported)
				.map(BindingDesc::getKey)
				.collect(toSet());

		if (!exportedKeys.isEmpty()) {
			Module exported = Modules.export(this, exportedKeys);
			// it would not recurse because we have the `finished` flag
			// and it's ok to reassign all of that below in the last moment

			bindings = exported.getBindings();
			bindingTransformers = exported.getBindingTransformers();
			bindingGenerators = exported.getBindingGenerators();
			multibinders = exported.getMultibinders();
		}
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
}
