package io.datakernel.di.module;

import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.core.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.ReflectionUtils.ProviderScanResults;
import static io.datakernel.di.util.ReflectionUtils.scanProviderMethods;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("UnusedReturnValue")
final class ModuleBuilderImpl<T> implements ModuleBuilderBinder<T> {
	private final List<BindingDesc> bindingDescs = new ArrayList<>();

	private Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());
	private Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	private final AtomicBoolean configured = new AtomicBoolean();

	@Nullable
	private volatile BindingDesc current = null;

	@Nullable
	private final StackTraceElement location;

	ModuleBuilderImpl() {
		// builder module is (and should be) never instantiated directly,
		// only by some factory methods (mainly the Module.create() ofc)
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		location = trace.length >= 3 ? trace[3] : null;
	}

	private void completeCurrent() {
		BindingDesc prev = current;
		if (prev != null) {
			bindingDescs.add(prev);
			current = null;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <U> ModuleBuilderBinder<U> bind(@NotNull Key<U> key) {
		checkState(!configured.get(), "Cannot bind after the module builder was used as a module");
		completeCurrent();
		current = new BindingDesc(key, new Binding<>(emptySet(), Preprocessor.TO_BE_GENERATED).at(LocationInfo.from(this)), UNSCOPED, false);
		return (ModuleBuilderBinder<U>) this;
	}

	private BindingDesc ensureCurrent() {
		checkState(!configured.get(), "Cannot use the module builder DSL after the module was used");
		BindingDesc desc = current;
		checkState(desc != null, "Cannot configure binding before bind(...) call");
		return desc;
	}

	@Override
	public ModuleBuilderBinder<T> named(@NotNull Name name) {
		BindingDesc desc = ensureCurrent();
		Key<?> key = desc.getKey();
		if (key.getName() != null) {
			throw new IllegalStateException("Already annotated with " + key.getName().getDisplayString());
		}
		desc.setKey(key.named(name));
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> in(@NotNull Scope[] scope) {
		BindingDesc desc = ensureCurrent();
		if (desc.getScope().length != 0) {
			throw new IllegalStateException("Already bound to scope " + getScopeDisplayString(desc.getScope()));
		}
		desc.setScope(scope);
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> in(@NotNull Scope scope, @NotNull Scope... scopes) {
		Scope[] joined = new Scope[scopes.length + 1];
		joined[0] = scope;
		System.arraycopy(scopes, 0, joined, 1, scopes.length);
		return in(joined);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final ModuleBuilderBinder<T> in(@NotNull Class<? extends Annotation> annotationClass, @NotNull Class<?>... annotationClasses) {
		return in(Stream.concat(Stream.of(annotationClass), Arrays.stream((Class<? extends Annotation>[]) annotationClasses)).map(Scope::of).toArray(Scope[]::new));
	}

	@Override
	public ModuleBuilderBinder<T> to(@NotNull Binding<? extends T> binding) {
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

	@Override
	public ModuleBuilderBinder<T> as(@NotNull Name name) {
		checkArgument(name.isMarkedBy(KeySetAnnotation.class), "Should be a key set name");
		checkState(!configured.get(), "Cannot use the module builder DSL after the module was used");

		Key<Set<Key<?>>> setKey = new Key<Set<Key<?>>>(name) {};

		BindingDesc desc = ensureCurrent();

		// binding constructor closes over the desc because the key could be modified after the .as() call
		bindingDescs.add(new BindingDesc(setKey, Binding.to(() -> singleton(desc.getKey())), UNSCOPED, false));

		multibinders.put(setKey, Multibinder.toSet());
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> withExtraDependencies(Set<Dependency> dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> withExtraDependencies(Dependency... dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> withExtraDependencies(Key<?>... dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> withExtraDependencies(Class<?>... dependencies) {
		BindingDesc current = ensureCurrent();
		current.setBinding(current.getBinding().addDependencies(dependencies));
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> export() {
		BindingDesc current = ensureCurrent();
		checkState(!current.isExported(), "Binding was already exported");
		current.setExported();
		return this;
	}

	private ModuleBuilder scan(@NotNull Class<?> moduleClass, @Nullable Object module) {
		checkState(!configured.get(), "Cannot add declarative bindings after the module builder was used as a module");
		completeCurrent();
		ProviderScanResults results = scanProviderMethods(moduleClass, module);
		bindingDescs.addAll(results.getBindingDescs());
		multibinders.putAll(results.getMultibinders());
		combineMultimap(bindingGenerators, results.getBindingGenerators());
		return this;
	}

	@Override
	public ModuleBuilder scan(Object container) {
		return scan(container.getClass(), container);
	}

	@Override
	public ModuleBuilder scan(Class<?> container) {
		return scan(container, null);
	}

	@Override
	public ModuleBuilder install(Collection<Module> modules) {
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

	@Override
	public <S, E extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Binding<E> binding) {
		checkState(!configured.get(), "Cannot install modules after the module builder was used as a module");

		completeCurrent();

		Key<Set<S>> set = Key.ofType(Types.parameterized(Set.class, setOf.getType()), setOf.getName());

		bindingDescs.add(new BindingDesc(set, binding.mapInstance(Collections::singleton), UNSCOPED, false));

		multibinders.put(set, Multibinder.toSet());
		return this;
	}

	@Override
	public <E> ModuleBuilder transform(int priority, BindingTransformer<E> bindingTransformer) {
		checkState(!configured.get(), "Cannot add transformers after the module builder was used as a module");
		completeCurrent();

		bindingTransformers.computeIfAbsent(priority, $ -> new HashSet<>()).add(bindingTransformer);
		return this;
	}

	@Override
	public <E> ModuleBuilder generate(Class<?> pattern, BindingGenerator<E> bindingGenerator) {
		checkState(!configured.get(), "Cannot add generators after the module builder was used as a module");
		completeCurrent();

		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>()).add(bindingGenerator);
		return this;
	}

	@Override
	public <E> ModuleBuilder multibind(Key<E> key, Multibinder<E> multibinder) {
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

	@Override
	public String toString() {
		return "BuilderModule(at " + (location != null ? location : "<unknown module location>") + ')';
	}
}
