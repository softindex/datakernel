package io.datakernel.di.module;

import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.*;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.*;
import java.util.stream.Collectors;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.ReflectionUtils.*;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

public abstract class AbstractModule implements Module {
	private boolean configured;

	private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());
	private final Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private final Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private final Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();
	private final List<BindingBuilder<Object>> builders = new ArrayList<>();

	protected final <T> void addBinding(Key<T> key, Binding<? extends T> binding) {
		addBinding(UNSCOPED, key, binding);
	}

	protected final <T> void addBinding(Scope scope, Key<T> key, Binding<? extends T> binding) {
		addBinding(new Scope[]{scope}, key, binding);
	}

	protected final <T> void addBinding(Scope[] scope, Key<T> key, Binding<? extends T> binding) {
		bindings.computeIfAbsent(scope, $ -> new HashMap<>())
				.get()
				.computeIfAbsent(key, $ -> new HashSet<>())
				.add(binding);
	}

	private void addKeyToSet(Name name, Key<?> key) {
		Key<Set<Key<?>>> setKey = new Key<Set<Key<?>>>(name) {};
		bind(setKey).toInstance(singleton(key));
		multibind(setKey, Multibinder.toSet());
	}

	protected final void addDeclarativeBindingsFrom(Object instance) {
		Class<?> cls = instance.getClass();

		for (Method method : getAnnotatedElements(cls, Provides.class, Class::getDeclaredMethods, false)) {
			Type type = Types.resolveTypeVariables(method.getGenericReturnType(), cls);
			TypeVariable<Method>[] typeVars = method.getTypeParameters();

			if (typeVars.length == 0) {
				Scope[] scope = getScope(method);
				Name name = nameOf(method);

				Key<Object> key = Key.ofType(type, name);
				addBinding(scope, key, bindingForMethod(instance, method));
				keySetsOf(method).forEach(keySet -> addKeyToSet(Name.of(keySet), key));
			} else {
				Set<TypeVariable<?>> unused = Arrays.stream(typeVars)
						.filter(typeVar -> !Types.contains(type, typeVar))
						.collect(Collectors.toSet());
				if (!unused.isEmpty()) {
					throw new IllegalStateException("Generic type variables " + unused + " are not used in return type");
				}
				generate(method.getReturnType(), (provider, scope, key) ->
						Types.matches(key.getType(), type) ?
								bindingForGenericMethod(instance, key, method) :
								null);
			}
		}
		for (Method method : getAnnotatedElements(cls, ProvidesIntoSet.class, Class::getDeclaredMethods, false)) {
			Scope[] scope = getScope(method);
			Name name = nameOf(method);
			Type type = Types.resolveTypeVariables(method.getGenericReturnType(), cls);

			Binding<Object> binding = bindingForMethod(instance, method);
			Factory<Object> factory = binding.getFactory();
			Key<Set<Object>> setKey = Key.ofType(Types.parameterized(Set.class, type), name);

			addBinding(scope, setKey, Binding.to(args -> singleton(factory.create(args)), binding.getDependencies()).at(binding.getLocation()));
			multibind(setKey, Multibinder.toSet());
			keySetsOf(method).forEach(keySet -> addKeyToSet(Name.of(keySet), setKey));
		}
	}

	@SuppressWarnings({"unchecked", "UnusedReturnValue"})
	public final class BindingBuilder<T> {
		private Scope[] scope = UNSCOPED;
		private Key<T> key;

		private Binding<? extends T> binding = (Binding<T>) BindingGraph.TO_BE_GENERATED;

		public BindingBuilder(Key<T> key) {
			this.key = key;
		}

		public BindingBuilder<T> annotatedWith(Name name) {
			if (key.getName() != null) {
				throw new IllegalStateException("Already annotated with " + key.getName().getDisplayString());
			}
			key = Key.ofType(key.getType(), name);
			return this;
		}

		public BindingBuilder<T> annotatedWith(Annotation annotation) {
			return annotatedWith(Name.of(annotation));
		}

		public BindingBuilder<T> annotatedWith(Class<? extends Annotation> annotationType) {
			return annotatedWith(Name.of(annotationType));
		}

		public BindingBuilder<T> in(Scope scope, Scope... scopes) {
			if (this.scope.length != 0) {
				throw new IllegalStateException("Already bound to scope " + Arrays.stream(this.scope).map(Scope::getDisplayString).collect(joining("->", "()", "")));
			}

			Scope[] ss = new Scope[scopes.length + 1];
			ss[0] = scope;
			System.arraycopy(scopes, 0, ss, 1, scopes.length);

			this.scope = ss;
			return this;
		}

		@SafeVarargs
		public final BindingBuilder<T> in(Class<? extends Annotation> annotationClass, Class<? extends Annotation>... annotationClasses) {
			return in(Scope.of(annotationClass), Arrays.stream(annotationClasses).map(Scope::of).toArray(Scope[]::new));
		}

		public BindingBuilder<T> to(Binding<? extends T> binding) {
			if (this.binding != BindingGraph.TO_BE_GENERATED) {
				throw new IllegalStateException("Already mapped to a binding");
			}
			this.binding = binding;
			return this;
		}

		public BindingBuilder<T> to(Factory<? extends T> factory) {
			return to(Binding.to(factory));
		}

		public BindingBuilder<T> to(Factory<? extends T> factory, Class<?>[] dependencies) {
			return to(Binding.to(factory, dependencies));
		}

		public BindingBuilder<T> to(Factory<? extends T> factory, Key<?>[] dependencies) {
			return to(Binding.to(factory, dependencies));
		}

		public BindingBuilder<T> to(Factory<? extends T> factory, Dependency[] dependencies) {
			return to(Binding.to(factory, dependencies));
		}

		public BindingBuilder<T> to(Class<? extends T> implementation) {
			return to(Binding.to(implementation));
		}

		public BindingBuilder<T> to(Key<? extends T> implementation) {
			return to(Binding.to(implementation));
		}

		public <U extends T> BindingBuilder<T> toInstance(@NotNull U instance) {
			return to(Binding.toInstance(instance));
		}

		public BindingBuilder<T> to(Constructor0<? extends T> constructor) {
			return to(Binding.to(constructor));
		}

		public <T1> BindingBuilder<T> to(Constructor1<T1, ? extends T> constructor,
				Class<T1> dependency1) {
			return to(Binding.to(constructor, dependency1));
		}

		public <T1, T2> BindingBuilder<T> to(Constructor2<T1, T2, ? extends T> constructor,
				Class<T1> dependency1, Class<T2> dependency2) {
			return to(Binding.to(constructor, dependency1, dependency2));
		}

		public <T1, T2, T3> BindingBuilder<T> to(Constructor3<T1, T2, T3, ? extends T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> BindingBuilder<T> to(Constructor4<T1, T2, T3, T4, ? extends T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> BindingBuilder<T> to(Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> BindingBuilder<T> to(Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5, Class<T6> dependency6) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		public <T1> BindingBuilder<T> to(Constructor1<T1, ? extends T> constructor,
				Key<T1> dependency1) {
			return to(Binding.to(constructor, dependency1));
		}

		public <T1, T2> BindingBuilder<T> to(Constructor2<T1, T2, ? extends T> constructor,
				Key<T1> dependency1, Key<T2> dependency2) {
			return to(Binding.to(constructor, dependency1, dependency2));
		}

		public <T1, T2, T3> BindingBuilder<T> to(Constructor3<T1, T2, T3, ? extends T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> BindingBuilder<T> to(Constructor4<T1, T2, T3, T4, ? extends T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> BindingBuilder<T> to(Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> BindingBuilder<T> to(Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		public BindingBuilder<T> as(@NotNull Class<? extends Annotation> annotationType) {
			return as(Name.of(annotationType));
		}

		public BindingBuilder<T> as(@NotNull Annotation annotation) {
			return as(Name.of(annotation));
		}

		public BindingBuilder<T> as(@NotNull String name) {
			return as(Name.of(name));
		}

		public BindingBuilder<T> as(@NotNull Name name) {
			checkArgument(name.isMarkedBy(KeySetAnnotation.class));

			Key<Set<Key<?>>> setKey = new Key<Set<Key<?>>>(name) {};
			bind(setKey).toInstance(singleton(key));
			multibind(setKey, Multibinder.toSet());
			return this;
		}
	}

	protected void configure() {
	}

	protected final void install(Module module) {
		bindings.addAll(module.getBindings(), multimapMerger());
		combineMultimap(bindingTransformers, module.getBindingTransformers());
		combineMultimap(bindingGenerators, module.getBindingGenerators());
		mergeMultibinders(multibinders, module.getMultibinders());
	}

	@SuppressWarnings("unchecked")
	protected final <T> BindingBuilder<T> bind(Key<T> key) {
		// to support abstract modules with generics
		Key<T> fullKey = Key.ofType(Types.resolveTypeVariables(key.getType(), getClass()), key.getName());
		BindingBuilder<T> builder = new BindingBuilder<>(fullKey);
		builders.add((BindingBuilder<Object>) builder);
		return builder;
	}

	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, T element) {
		bindIntoSet(setOf, Binding.toInstance(element));
	}

	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Key<T> item) {
		bindIntoSet(setOf, Binding.to(item));
	}

	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Binding<T> binding) {
		Key<Set<S>> set = Key.ofType(Types.parameterized(Set.class, setOf.getType()), setOf.getName());
		multibind(set, Multibinder.toSet());
		bind(set).to(binding.mapInstance(Collections::singleton));
	}

	@SuppressWarnings("unchecked")
	protected final <T> BindingBuilder<T> bind(Class<T> type) {
		BindingBuilder<T> builder = new BindingBuilder<>(Key.of(type));
		builders.add((BindingBuilder<Object>) builder);
		return builder;
	}

	protected final <T> void multibind(Key<T> key, Multibinder<T> multibinder) {
		multibinders.put(key, multibinder);
	}

	protected final <T> void generate(Class<?> pattern, BindingGenerator<T> bindingGenerator) {
		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>()).add(bindingGenerator);
	}

	protected final <T> void transform(int priority, BindingTransformer<T> bindingTransformer) {
		bindingTransformers.computeIfAbsent(priority, $ -> new HashSet<>()).add(bindingTransformer);
	}

	synchronized private void doConfigure() {
		if (configured) {
			return;
		}
		configured = true;
		configure();
		addDeclarativeBindingsFrom(this);
		builders.forEach(builder -> addBinding(builder.scope, builder.key, builder.binding));
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
		doConfigure();
		return bindings;
	}

	@Override
	public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		doConfigure();
		return bindingTransformers;
	}

	@Override
	public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		doConfigure();
		return bindingGenerators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		doConfigure();
		return multibinders;
	}
}
