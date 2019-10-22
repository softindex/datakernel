package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static io.datakernel.di.impl.CompiledBinding.missingOptionalBinding;
import static io.datakernel.di.module.BindingType.*;
import static io.datakernel.di.util.ReflectionUtils.scanClassHierarchy;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("UnusedReturnValue")
final class ModuleBuilderImpl<T> implements ModuleBuilderBinder<T> {
	private static final Binding<?> TO_BE_GENERATED = new Binding<>(emptySet(), (compiledBindings, threadsafe, scope, slot) -> missingOptionalBinding());

	private final List<BindingDesc> bindingDescs = new ArrayList<>();

	private Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings = Trie.leaf(new HashMap<>());
	private Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	private final AtomicBoolean configured = new AtomicBoolean();

	@Nullable
	private volatile BindingDesc current = null;

	private final String name;
	@Nullable
	private final StackTraceElement location;

	ModuleBuilderImpl() {
		// builder module is (and should be) never instantiated directly,
		// only by Module.create() and AbstractModule actually
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		location = trace.length >= 3 ? trace[3] : null;
		name = getClass().getName();
	}

	ModuleBuilderImpl(String name, @Nullable StackTraceElement location) {
		this.name = name;
		this.location = location;
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
		current = new BindingDesc(key, TO_BE_GENERATED);
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
		checkState(desc.getBinding() == TO_BE_GENERATED, "Already mapped to a binding");
		if (binding.getLocation() == null) {
			binding.at(LocationInfo.from(this));
		}
		desc.setBinding(binding);
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> export() {
		BindingDesc current = ensureCurrent();
		checkState(!current.isExported(), "Binding was already exported");
		current.setExported();
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> eagerly() {
		BindingDesc current = ensureCurrent();
		checkState(current.getType() == COMMON, "Binding was already set to eager or transient");
		current.setType(EAGER);
		return this;
	}

	@Override
	public ModuleBuilderBinder<T> transiently() {
		BindingDesc current = ensureCurrent();
		checkState(current.getType() == COMMON, "Binding was already set to transient or eager");
		current.setType(TRANSIENT);
		return this;
	}

	@Override
	public ModuleBuilder scan(@NotNull Class<?> moduleClass, @Nullable Object module) {
		checkState(!configured.get(), "Cannot add declarative bindings after the module builder was used as a module");
		return install(scanClassHierarchy(moduleClass, module).values());
	}

	@Override
	public ModuleBuilder install(Collection<Module> modules) {
		checkState(!configured.get(), "Cannot install modules after the module builder was used as a module");
		completeCurrent();
		for (Module module : modules) {
			bindings.addAll(module.getBindings(), bindingMultimapMerger());
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

		bindingDescs.add(new BindingDesc(set, binding.mapInstance(Collections::singleton)));

		multibinders.put(set, Multibinder.toSet());
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> ModuleBuilder transform(int priority, BindingTransformer<E> bindingTransformer) {
		checkState(!configured.get(), "Cannot add transformers after the module builder was used as a module");
		completeCurrent();

		bindingTransformers.computeIfAbsent(priority, $ -> new HashSet<>())
				.add((bindings, scope, key, binding) -> {
					Binding<Object> transformed = (Binding<Object>) bindingTransformer.transform(bindings, scope, (Key<E>) key, (Binding<E>) binding);
					if (!binding.equals(transformed) && transformed.getLocation() == null) {
						transformed.at(LocationInfo.from(this));
					}
					return transformed;
				});
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> ModuleBuilder generate(Class<?> pattern, BindingGenerator<E> bindingGenerator) {
		checkState(!configured.get(), "Cannot add generators after the module builder was used as a module");
		completeCurrent();

		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>())
				.add((bindings, scope, key) -> {
					Binding<Object> generated = (Binding<Object>) bindingGenerator.generate(bindings, scope, (Key<E>) key);
					if (generated != null && generated.getLocation() == null) {
						generated.at(LocationInfo.from(this));
					}
					return generated;
				});
		return this;
	}

	@Override
	public <E> ModuleBuilder multibind(Key<E> key, Multibinder<E> multibinder) {
		checkState(!configured.get(), "Cannot add multibinders after the module builder was used as a module");
		completeCurrent();

		multibinders.put(key, multibinder);
		return this;
	}

	@SuppressWarnings("unchecked")
	private void finish() {
		if (!configured.compareAndSet(false, true)) {
			return;
		}
		completeCurrent(); // finish the last binding

		bindingDescs.forEach(b -> {
			BindingSet<?> bindingSet = bindings
					.computeIfAbsent(b.getScope(), $1 -> new HashMap<>())
					.get()
					.computeIfAbsent(b.getKey(), $ -> new BindingSet<>(new HashSet<>(), COMMON));

			bindingSet.setType(b.getType());

			Binding<?> binding = b.getBinding();
			if (binding != TO_BE_GENERATED) {
				bindingSet.getBindings().add((Binding) binding);
			}
		});

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

	@Override
	public String toString() {
		return name + "(at " + (location != null ? location : "<unknown module location>") + ')';
	}
}
