package io.datakernel.di.module;

import io.datakernel.di.*;
import io.datakernel.di.util.BindingUtils;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
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
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

public abstract class AbstractModule implements Module {

	private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());

	private final Map<Integer, BindingTransformer<?>> bindingTransformers = new HashMap<>();
	private final Map<Type, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private final Map<Key<?>, ConflictResolver<?>> conflictResolvers = new HashMap<>();

	@Nullable
	private List<BindingBuilder<?>> builders = new ArrayList<>();

	public AbstractModule() {
		configure();
		addDeclarativeBindingsFrom(this);
	}

	protected void addDeclarativeBindingsFrom(Object instance) {
		Class<?> cls = instance.getClass();
		Key<?> moduleType = Key.of(cls);

		for (Method provider : getAnnotatedElements(cls, Provides.class, Class::getDeclaredMethods)) {
			TypeVariable<Method>[] typeVars = provider.getTypeParameters();
			if (typeVars.length == 0) {
				Annotation[] annotations = provider.getDeclaredAnnotations();
				Key<Object> key = keyOf(moduleType, provider.getGenericReturnType(), annotations);
				bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
						.get()
						.computeIfAbsent(key, $ -> new HashSet<>())
						.add(bindingForMethod(instance, provider));
			} else {
				Type genericReturnType = provider.getGenericReturnType();
				for (TypeVariable<Method> typeVar : typeVars) {
					if (typeVar.getBounds().length != 1 && typeVar.getBounds()[0] != Object.class) {
						throw new RuntimeException("Bounded type vars are not supported yet");
					}
					if (!ReflectionUtils.contains(genericReturnType, typeVar)) {
						throw new RuntimeException("generic type variable " + typeVar + " is not used in return type");
					}
				}

				generate(genericReturnType, (scope, key, context) -> bindingForGenericMethod(instance, key, provider));
			}
		}
		for (Method provider : getAnnotatedElements(cls, ProvidesIntoSet.class, Class::getDeclaredMethods)) {
			Annotation[] annotations = provider.getDeclaredAnnotations();
			Key<Object> key = keyOf(moduleType, provider.getGenericReturnType(), annotations);

			Binding<Object> binding = bindingForMethod(instance, provider);
			Factory<Object> factory = binding.getFactory();
			Key<Set<Object>> setKey = Key.ofType(parameterized(Set.class, key.getType()), key.getName());

			bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
					.get()
					.computeIfAbsent(setKey, $ -> new HashSet<>())
					.add(Binding.of(binding.getDependencies(), args -> singleton(factory.create(args))).at(binding.getLocation()));

			resolve(setKey, multibinderToSet());
		}
	}

	@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "unchecked"})
	public final class BindingBuilder<T> {
		private Scope[] scope = UNSCOPED;
		private Key<T> key;

		private Binding<T> binding = (Binding<T>) BindingUtils.PHANTOM;

		public BindingBuilder(Key<T> key) {
			this.key = key;
		}

		public BindingBuilder<T> annotatedWith(Name name) {
			if (key.getName() != null) {
				throw new RuntimeException("already annotated with some name");
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
				throw new RuntimeException("already bound to some scope");
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

		public void toBinding(Binding<T> binding) {
			if (this.binding != BindingUtils.PHANTOM) {
				throw new RuntimeException("already mapped to some binding");
			}
			this.binding = binding;
		}

		public void to(Factory<T> factory, Key<?>... dependencies) {
			toBinding(Binding.of(dependencies, factory).at(getLocation(BindingBuilder.class)));
		}

		public void to(Factory<T> factory, List<Key<?>> dependencies) {
			to(factory, dependencies.toArray(new Key[0]));
		}

		public void to(Class<? extends T> implementation) {
			to(Key.of(implementation));
		}

		public void to(Key<? extends T> implementation) {
			to(impl -> impl, implementation);
		}

		public void to(Constructor0<T> constructor) {
			to(constructor, asList());
		}

		public <T1> void to(Constructor1<T1, T> constructor,
							Key<T1> dependency1) {
			to(constructor, asList(dependency1));
		}

		public <T1, T2> void to(Constructor2<T1, T2, T> constructor,
								Key<T1> dependency1, Key<T2> dependency2) {
			to(constructor, asList(dependency1, dependency2));
		}

		public <T1, T2, T3> void to(Constructor3<T1, T2, T3, T> constructor,
									Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
			to(constructor, asList(dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> void to(Constructor4<T1, T2, T3, T4, T> constructor,
										Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
			to(constructor, asList(dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> void to(Constructor5<T1, T2, T3, T4, T5, T> constructor,
											Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
			to(constructor, asList(dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> void to(Constructor6<T1, T2, T3, T4, T5, T6, T> constructor,
												Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
			to(constructor, asList(dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		public <T1> void to(Constructor1<T1, T> constructor,
							Class<T1> dependency1) {
			to(constructor, Key.of(dependency1));
		}

		public <T1, T2> void to(Constructor2<T1, T2, T> constructor,
								Class<T1> dependency1, Class<T2> dependency2) {
			to(constructor, Key.of(dependency1), Key.of(dependency2));
		}

		public <T1, T2, T3> void to(Constructor3<T1, T2, T3, T> constructor,
									Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3));
		}

		public <T1, T2, T3, T4> void to(Constructor4<T1, T2, T3, T4, T> constructor,
										Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4));
		}

		public <T1, T2, T3, T4, T5> void to(Constructor5<T1, T2, T3, T4, T5, T> constructor,
											Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> void to(Constructor6<T1, T2, T3, T4, T5, T6, T> constructor,
												Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5, Class<T6> dependency6) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5), Key.of(dependency6));
		}

		@SuppressWarnings("unchecked")
		public void toInstance(T instance) {
			toBinding(Binding.constant(instance).at(getLocation(BindingBuilder.class))
					.apply(injectingInitializer((Key<T>) Key.of(instance.getClass()))));
			// default injecting transformer uses the key that the binding is bound to
			// here we also add injects from impl type, double injects for parent type but whatever
			// eg key Launcher -> binding for HttpServerLauncher
			// default will look at injects from class Launcher,
			// this one also looks in HttpServerLauncher
		}
	}

	protected void configure() {
	}

	protected void install(Module module) {
		bindings.addAll(module.getBindingsMultimap(), multimapMerger());
		combineMultimap(bindingGenerators, module.getBindingGenerators());
		mergeConflictResolvers(conflictResolvers, module.getConflictResolvers());
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

	protected final <T> void resolve(Key<T> key, ConflictResolver<T> conflictResolver) {
		mergeConflictResolvers(conflictResolvers, singletonMap(key, conflictResolver));
	}

	protected final <T> void generate(Type pattern, BindingGenerator<T> conflictResolver) {
		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>()).add(conflictResolver);
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
	public Map<Key<?>, ConflictResolver<?>> getConflictResolvers() {
		return conflictResolvers;
	}
}
