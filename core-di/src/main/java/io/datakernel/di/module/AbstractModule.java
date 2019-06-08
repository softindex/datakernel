package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Name;
import io.datakernel.di.Scope;
import io.datakernel.di.util.BindingUtils;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.*;

import static io.datakernel.di.module.Modules.multibinderToSet;
import static io.datakernel.di.util.ReflectionUtils.*;
import static io.datakernel.di.util.ScopedValue.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

public abstract class AbstractModule implements Module {

	private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());

	private final Map<Integer, BindingTransformer<?>> bindingTransformers = new HashMap<>();
	private final Map<Type, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private final Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	@Nullable
	private List<BindingBuilder<?>> builders = new ArrayList<>();

	public AbstractModule() {
		configure();
		addDeclarativeBindingsFrom(this);
	}

	protected final void addDeclarativeBindingsFrom(Object instance) {
		Class<?> cls = instance.getClass();
		Key<?> moduleType = Key.of(cls);

		for (Method method : getAnnotatedElements(cls, Provides.class, Class::getDeclaredMethods)) {
			TypeVariable<Method>[] typeVars = method.getTypeParameters();
			if (typeVars.length == 0) {
				Annotation[] annotations = method.getDeclaredAnnotations();
				Key<Object> key = keyOf(moduleType, method.getGenericReturnType(), annotations);
				bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
						.get()
						.computeIfAbsent(key, $ -> new HashSet<>())
						.add(bindingForMethod(instance, method));
			} else {
				Type genericReturnType = method.getGenericReturnType();
				for (TypeVariable<Method> typeVar : typeVars) {
					if (typeVar.getBounds().length != 1 && typeVar.getBounds()[0] != Object.class) {
						throw new IllegalArgumentException("Bounded type vars are not supported yet");
					}
					if (!ReflectionUtils.contains(genericReturnType, typeVar)) {
						throw new IllegalStateException("Generic type variable " + typeVar + " must be used in return type");
					}
				}

				generate(genericReturnType, (scope, key, provider) -> bindingForGenericMethod(instance, key, method));
			}
		}
		for (Method method : getAnnotatedElements(cls, ProvidesIntoSet.class, Class::getDeclaredMethods)) {
			Annotation[] annotations = method.getDeclaredAnnotations();
			Key<Object> key = keyOf(moduleType, method.getGenericReturnType(), annotations);

			Binding<Object> binding = bindingForMethod(instance, method);
			Factory<Object> factory = binding.getFactory();
			Key<Set<Object>> setKey = Key.ofType(parameterized(Set.class, key.getType()), key.getName());

			bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
					.get()
					.computeIfAbsent(setKey, $ -> new HashSet<>())
					.add(Binding.of(args -> singleton(factory.create(args)), binding.getDependencies()).at(binding.getLocation()));

			multibind(setKey, multibinderToSet());
		}
	}

	@SuppressWarnings({"unchecked", "UnusedReturnValue"})
	public final class BindingBuilder<T> {
		private Scope[] scope = UNSCOPED;
		private Key<T> key;

		private Binding<T> binding = (Binding<T>) BindingUtils.PHANTOM;

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

		public BindingBuilder<T> to(Binding<T> binding) {
			if (this.binding != BindingUtils.PHANTOM) {
				throw new IllegalStateException("Already mapped to a binding");
			}
			this.binding = binding.at(getLocation(BindingBuilder.class));
			return this;
		}

		public BindingBuilder<T> to(Factory<T> factory, Key<?>... dependencies) {
			return to(Binding.of(factory, dependencies));
		}

		public BindingBuilder<T> to(Factory<T> factory, List<Key<?>> dependencies) {
			return to(factory, dependencies.toArray(new Key[0]));
		}

		public BindingBuilder<T> to(Class<? extends T> implementation) {
			return to(Key.of(implementation));
		}

		public BindingBuilder<T> to(Key<? extends T> implementation) {
			return to(impl -> impl, implementation);
		}

		public BindingBuilder<T> to(Constructor0<T> constructor) {
			return to(Binding.of(constructor));
		}

		public <T1> BindingBuilder<T> to(Constructor1<T1, T> constructor,
				Key<T1> dependency1) {
			return to(Binding.to(constructor, dependency1));
		}

		public <T1, T2> BindingBuilder<T> to(Constructor2<T1, T2, T> constructor,
				Key<T1> dependency1, Key<T2> dependency2) {
			return to(Binding.to(constructor, dependency1, dependency2));
		}

		public <T1, T2, T3> BindingBuilder<T> to(Constructor3<T1, T2, T3, T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> BindingBuilder<T> to(Constructor4<T1, T2, T3, T4, T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> BindingBuilder<T> to(Constructor5<T1, T2, T3, T4, T5, T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> BindingBuilder<T> to(Constructor6<T1, T2, T3, T4, T5, T6, T> constructor,
				Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		public <T1> BindingBuilder<T> to(Constructor1<T1, T> constructor,
				Class<T1> dependency1) {
			return to(Binding.of(constructor, dependency1));
		}

		public <T1, T2> BindingBuilder<T> to(Constructor2<T1, T2, T> constructor,
				Class<T1> dependency1, Class<T2> dependency2) {
			return to(Binding.of(constructor, dependency1, dependency2));
		}

		public <T1, T2, T3> BindingBuilder<T> to(Constructor3<T1, T2, T3, T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3) {
			return to(Binding.of(constructor, dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> BindingBuilder<T> to(Constructor4<T1, T2, T3, T4, T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4) {
			return to(Binding.of(constructor, dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> BindingBuilder<T> to(Constructor5<T1, T2, T3, T4, T5, T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5) {
			return to(Binding.of(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> BindingBuilder<T> to(Constructor6<T1, T2, T3, T4, T5, T6, T> constructor,
				Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5, Class<T6> dependency6) {
			return to(Binding.of(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		public BindingBuilder<T> toInstance(@NotNull T instance) {
			return to(Binding.toInstance(instance).at(getLocation(BindingBuilder.class)));
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
			multibinders.put(new Key<Set<Key<?>>>(name) {}, multibinderToSet());
			bind(new Key<Set<Key<?>>>(name) {}).toInstance(singleton(key));
			return this;
		}
	}

	protected void configure() {
	}

	protected final void install(Module module) {
		bindings.addAll(module.getBindingsMultimap(), multimapMerger());
		combineMultimap(bindingGenerators, module.getBindingGenerators());
		mergeMultibinders(multibinders, module.getMultibinders());
		mergeBindingTransformers(bindingTransformers, module.getBindingTransformers());
	}

	protected final <T> BindingBuilder<T> bind(Key<T> key) {
		Key<T> fullKey = Key.ofType(resolveGenerics(key.getType(), Key.of(getClass())), key.getName());
		BindingBuilder<T> builder = new BindingBuilder<>(fullKey);
		if (builders == null) {
			throw new AssertionError("cannot call bind after the module was used");
		}
		builders.add(builder);
		return builder;

	}

	protected final <T> BindingBuilder<T> bind(Class<T> type) {
		BindingBuilder<T> builder = new BindingBuilder<>(Key.of(type));
		if (builders == null) {
			throw new AssertionError("cannot call bind after the module was used");
		}
		builders.add(builder);
		return builder;
	}

	protected final <T> void multibind(Key<T> key, Multibinder<T> multibinder) {
		multibinders.put(key, multibinder);
	}

	protected final <T> void generate(Type pattern, BindingGenerator<T> bindingGenerator) {
		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>()).add(bindingGenerator);
	}

	protected final <T> void transform(int priority, BindingTransformer<T> bindingTransformer) {
		bindingTransformers.put(priority, bindingTransformer);
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
		if (builders != null) {
			for (BindingBuilder<?> builder : builders) {
				bindings.computeIfAbsent(builder.scope, $1 -> new HashMap<>())
						.get()
						.computeIfAbsent(builder.key, $ -> new HashSet<>())
						.add(builder.binding);
			}
			builders = null;
		}
		return bindings;
	}

	@Override
	public Map<Integer, BindingTransformer<?>> getBindingTransformers() {
		return bindingTransformers;
	}

	@Override
	public Map<Type, Set<BindingGenerator<?>>> getBindingGenerators() {
		return bindingGenerators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return multibinders;
	}
}
