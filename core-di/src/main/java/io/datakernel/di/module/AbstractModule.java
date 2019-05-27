package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.LocationInfo;
import io.datakernel.di.Scope;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.di.module.Modules.multibinderToSet;
import static io.datakernel.di.util.ReflectionUtils.*;
import static io.datakernel.di.util.Utils.multimapMerger;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public abstract class AbstractModule implements Module {

	private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());
	private final Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>();

	public AbstractModule() {
		configure();
		addDeclatativeBindings();
	}

	@SuppressWarnings("unchecked")
	private void addDeclatativeBindings() {
		for (Method method : getClass().getDeclaredMethods()) {
			Annotation[] annotations = method.getDeclaredAnnotations();
			Key<Object> key = keyOf(method.getGenericReturnType(), annotations);
			if (method.isAnnotationPresent(Provides.class)) {
				bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
						.get()
						.computeIfAbsent(key, $ -> new HashSet<>())
						.add(bindingForMethod(this, method).apply(injectingInitializer(key)));
			}
			if (method.isAnnotationPresent(ProvidesIntoSet.class)) {
				Binding<Object> binding = bindingForMethod(this, method).apply(injectingInitializer(key));
				Factory<Object> factory = binding.getFactory();
				Key<Set<Object>> setKey = Key.ofType(parameterized(Set.class, key.getType()), key.getName());

				bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
						.get()
						.computeIfAbsent(setKey, $ -> new HashSet<>())
						.add(Binding.of(binding.getDependencies(), args -> singleton(factory.create(args)), binding.getLocation()));

				conflictResolvers.putIfAbsent(setKey, (Function) multibinderToSet());
			}
		}
	}

	private <T> void addBinding(Scope[] scope, Key<T> key, Binding<T> binding) {
		addBinding(scope, key, key, binding);
	}

	private <T> void addBinding(Scope[] scope, Key<T> key, Key<? extends T> targetKey, Binding<T> binding) {
		bindings.computeIfAbsent(scope, $ -> new HashMap<>())
				.get()
				.computeIfAbsent(key, $ -> new HashSet<>())
				.add(binding.apply(injectingInitializer(targetKey)));
	}

	@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument"})
	public class BindingBuilder<T> {
		private final Key<T> key;
		private final Scope[] scope;

		public BindingBuilder(Key<T> key, Scope[] scope) {
			this.key = key;
			this.scope = scope;
		}

		public BindingBuilder<T> annotatedWith(Annotation annotation) {
			return new BindingBuilder<>(Key.ofType(key.getType(), annotation), scope);
		}

		public BindingBuilder<T> annotatedWith(Class<? extends Annotation> annotationType) {
			return new BindingBuilder<>(Key.ofType(key.getType(), annotationType), scope);
		}

		public BindingBuilder<T> in(Scope scope, Scope... scopes) {
			if (this.scope.length != 0) {
				throw new RuntimeException("already bound to some scope");
			}
			Scope[] ss = new Scope[scopes.length + 1];
			ss[0] = scope;
			System.arraycopy(scopes, 0, ss, 1, scopes.length);
			return new BindingBuilder<>(key, ss);
		}

		public BindingBuilder<T> in(Class<? extends Annotation> annotationClass) {
			return in(Scope.of(annotationClass));
		}

		public void to(Factory<T> factory, Key<?>... dependencies) {
			addBinding(scope, key, Binding.of(dependencies, factory, getLocation()));
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
			addBinding(scope, key, Key.of((Class<? extends T>) instance.getClass()), Binding.of(new Key[]{}, $ -> instance, getLocation()));
		}

		public void implicitly() {
			Binding<T> binding = ReflectionUtils.generateImplicitBinding(key);
			if (binding == null) {
				throw new RuntimeException("requested implicit binding for " + key + " but it had none");
			}
			binding.setLocation(getLocation()); // overriding the location, eh
			addBinding(scope, key, binding);
		}

		@Nullable
		private LocationInfo getLocation() {
			return Arrays.stream(Thread.currentThread().getStackTrace())
					.skip(2)
					.filter(trace -> !trace.getClassName().equals(BindingBuilder.class.getName()))
					.findFirst()
					.map(LocationInfo::from)
					.orElse(null);
		}
	}

	protected void configure() {
	}

	protected void install(Module module) {
		bindings.addAll(module.getBindingsMultimap(), multimapMerger());
		module.getConflictResolvers().forEach((k, v) -> conflictResolvers.merge(k, v, ($, $2) -> {
			throw new RuntimeException("more than one conflict resolver per key");
		}));
	}

	protected final <T> BindingBuilder<T> bind(Key<T> key) {
		return new BindingBuilder<>(key, new Scope[0]);
	}

	protected final <T> BindingBuilder<T> bind(Class<T> type) {
		return bind(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	protected final <T> void resolveConflicts(Key<T> key, Function<Set<Binding<T>>, Binding<T>> conflictResolver) {
		conflictResolvers.merge(key, (Function) conflictResolver, ($, $2) -> {
			throw new RuntimeException("more than one conflict resolver per key");
		});
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
		return bindings;
	}

	@Override
	public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
		return conflictResolvers;
	}
}
