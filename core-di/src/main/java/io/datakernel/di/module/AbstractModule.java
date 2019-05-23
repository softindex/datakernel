package io.datakernel.di.module;

import io.datakernel.di.*;
import io.datakernel.di.Binding.Factory;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.ReflectionUtils;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.function.Function;

import static java.util.Arrays.asList;

public abstract class AbstractModule implements Module {

	private final ScopedBindings bindings = ScopedBindings.create();
	private final Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>();

	public AbstractModule() {
		configure();

//		ReflectionUtils.getDeclatativeBindings(this)
//				.forEach((scope, subBindings) -> {
//					Map<Key<?>, Set<Binding<?>>> actualBindings = scope != null ?
//							scopeBindings.computeIfAbsent(scope, $ -> new HashMap<>()) :
//							bindings;
//					subBindings.forEach(binding -> actualBindings.computeIfAbsent(binding.getKey(), $ -> new HashSet<>()).add(binding));
//				});
	}

	protected void configure() {
	}

	protected void install(Module module) {
//		combineMultimap(bindings, module.getBindings());
//		module.getConflictResolvers().forEach((k, v) -> conflictResolvers.merge(k, v, ($, $2) -> {
//			throw new RuntimeException("more than one conflict resolver per key");
//		}));
//		module.getScopeBindings().forEach((scope, bindings) -> combineMultimap(scopeBindings.computeIfAbsent(scope, $ -> new HashMap<>()), bindings));
	}

	@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "unchecked"})
	public class BindingBuilder<T> {
		private final Key<T> key;
		private final Scope[] scopes;

		public BindingBuilder(Key<T> key, Scope[] scopes) {
			this.key = key;
			this.scopes = scopes;
		}

		public BindingBuilder<T> annotatedWith(Annotation annotation) {
			return new BindingBuilder<>(Key.ofType(key.getType(), annotation), scopes);
		}

		public BindingBuilder<T> annotatedWith(Class<? extends Annotation> annotationType) {
			return new BindingBuilder<>(Key.ofType(key.getType(), annotationType), scopes);
		}

		public BindingBuilder<T> in(Scope scope, Scope... scopes) {
			if (this.scopes.length != 0) {
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
			bindings.resolve(scopes).add(key, Binding.of(dependencies, factory, getLocation())
					.apply(ReflectionUtils.injectingInitializer(key)));
		}

		public void to(Factory<T> factory, List<Key<?>> dependencies) {
			to(factory, dependencies.toArray(new Key[0]));
		}

		public void to(Class<? extends T> implementation) {
			to(Key.of(implementation));
		}

		public void to(Key<? extends T> implementation) {
			to(args -> (T) args[0], asList(implementation));
		}

		public void to(Constructor0<T> constructor) {
			to(args -> constructor.create(), asList());
		}

		public <T1> void to(Constructor1<T1, T> constructor,
							Key<T1> dependency1) {
			to(args -> constructor.create((T1) args[0]), asList(dependency1));
		}

		public <T1, T2> void to(Constructor2<T1, T2, T> constructor,
								Key<T1> dependency1, Key<T2> dependency2) {
			to(args -> constructor.create((T1) args[0], (T2) args[1]), asList(dependency1, dependency2));
		}

		public <T1, T2, T3> void to(Constructor3<T1, T2, T3, T> constructor,
									Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2]), asList(dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> void to(Constructor4<T1, T2, T3, T4, T> constructor,
										Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2], (T4) args[3]), asList(dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> void to(Constructor5<T1, T2, T3, T4, T5, T> constructor,
											Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2], (T4) args[3], (T5) args[4]), asList(dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> void to(Constructor6<T1, T2, T3, T4, T5, T6, T> constructor,
												Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2], (T4) args[3], (T5) args[4], (T6) args[5]), asList(dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
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

		public void toInstance(T instance) {
			to($ -> instance, asList());
		}

		public void overridenLater(Dependency... dependencies) {
			bindings.resolve(scopes).add(key, Binding.of(dependencies, $ -> {
				throw new RuntimeException("binding for " + key + " was not overriden when entering the scope as it was supposed to");
			}, getLocation()));
		}

		public void overridenLater(List<Dependency> dependencies) {
			overridenLater(dependencies.toArray(new Dependency[0]));
		}

		public void require() {
			Binding<T> binding = ReflectionUtils.generateImplicitBinding(key);
			if (binding == null) {
				throw new RuntimeException("requested automatic creation of " + key + " but it had no implicit bindings");
			}
			binding.setLocation(getLocation()); // overriding the location, eh
			bindings.resolve(scopes).add(key, binding);
		}

		@Nullable
		private LocationInfo getLocation() {
			return Arrays.stream(Thread.currentThread().getStackTrace())
					.skip(2)
					.filter(trace -> !trace.getClassName().equals(BindingBuilder.class.getName()))
					.findFirst()
					.flatMap(trace -> {
						try {
							return Optional.of(new LocationInfo(Class.forName(trace.getClassName()), trace.toString()));
						} catch (ClassNotFoundException ignored) {
							return Optional.empty();
						}
					})
					.orElse(null);
		}
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
	public ScopedBindings getBindings() {
		return bindings;
	}

	@Override
	public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
		return conflictResolvers;
	}
}
