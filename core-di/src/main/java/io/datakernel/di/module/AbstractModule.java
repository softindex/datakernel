package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Dependency;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.util.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.di.util.Utils.combineMultimap;
import static java.util.Arrays.asList;

public abstract class AbstractModule implements Module {
	private final Map<Key<?>, Set<Binding<?>>> bindings = new HashMap<>();
	private final Map<Scope, Map<Key<?>, Set<Binding<?>>>> scopeBindings = new HashMap<>();
	private final Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>();

	public AbstractModule() {
		configure();
		ReflectionUtils.getDeclatativeBindings(this)
				.forEach(binding -> bindings.computeIfAbsent(binding.getKey(), $ -> new HashSet<>()).add(binding));
	}

	protected void configure() {
	}

	protected void install(Module module) {
		combineMultimap(bindings, module.getBindings());
		module.getConflictResolvers().forEach((k, v) -> conflictResolvers.merge(k, v, ($, $2) -> {
			throw new RuntimeException("more than one conflict resolver per key");
		}));
		module.getScopeBindings().forEach((scope, bindings) -> combineMultimap(scopeBindings.computeIfAbsent(scope, $ -> new HashMap<>()), bindings));
	}

	@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "unchecked"})
	public class BindingBuilder<T> {
		private final Key<T> key;
		private final Scope scope;

		public BindingBuilder(Key<T> key, Scope scope) {
			this.key = key;
			this.scope = scope;
		}

		public BindingBuilder<T> annotatedWith(Annotation annotation) {
			return new BindingBuilder<>(Key.of(key.getTypeT(), annotation), scope);
		}

		public BindingBuilder<T> annotatedWith(Class<? extends Annotation> annotationType) {
			return new BindingBuilder<>(Key.of(key.getTypeT(), annotationType), scope);
		}

		public BindingBuilder<T> in(Scope scope) {
			return new BindingBuilder<>(key, scope);
		}

		public BindingBuilder<T> in(Class<? extends Annotation> annotationClass) {
			return in(Scope.of(annotationClass));
		}

		public void to(Binding.Constructor<T> constructor, List<Key<?>> dependencies) {
			to(constructor, dependencies.toArray(new Key[0]));
		}

		public void to(Binding.Constructor<T> constructor, Key<?>... dependencies) {
			bindings.computeIfAbsent(key, $ -> new HashSet<>())
					.add(new Binding<>(key, Arrays.stream(dependencies).map(k -> new Dependency(k, true)).toArray(Dependency[]::new), constructor));
		}

		public void to(Class<? extends T> implementation) {
			to(Key.of(implementation));
		}

		public void to(TypeT<? extends T> implementation) {
			to(Key.of(implementation));
		}

		public void to(Key<? extends T> implementation) {
			to(args -> (T) args[0], asList(implementation));
		}

		public void to(TupleConstructor0<T> constructor) {
			to(args -> constructor.create(), asList());
		}

		public <T1> void to(TupleConstructor1<T1, T> constructor,
							Key<T1> dependency1) {
			to(args -> constructor.create((T1) args[0]), asList(dependency1));
		}

		public <T1, T2> void to(TupleConstructor2<T1, T2, T> constructor,
								Key<T1> dependency1, Key<T2> dependency2) {
			to(args -> constructor.create((T1) args[0], (T2) args[1]), asList(dependency1, dependency2));
		}

		public <T1, T2, T3> void to(TupleConstructor3<T1, T2, T3, T> constructor,
									Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2]), asList(dependency1, dependency2, dependency3));
		}

		public <T1, T2, T3, T4> void to(TupleConstructor4<T1, T2, T3, T4, T> constructor,
										Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2], (T4) args[3]), asList(dependency1, dependency2, dependency3, dependency4));
		}

		public <T1, T2, T3, T4, T5> void to(TupleConstructor5<T1, T2, T3, T4, T5, T> constructor,
											Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2], (T4) args[3], (T5) args[4]), asList(dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> void to(TupleConstructor6<T1, T2, T3, T4, T5, T6, T> constructor,
												Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
			to(args -> constructor.create((T1) args[0], (T2) args[1], (T3) args[2], (T4) args[3], (T5) args[4], (T6) args[5]), asList(dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		public <T1> void to(TupleConstructor1<T1, T> constructor,
							Class<T1> dependency1) {
			to(constructor, Key.of(dependency1));
		}

		public <T1, T2> void to(TupleConstructor2<T1, T2, T> constructor,
								Class<T1> dependency1, Class<T2> dependency2) {
			to(constructor, Key.of(dependency1), Key.of(dependency2));
		}

		public <T1, T2, T3> void to(TupleConstructor3<T1, T2, T3, T> constructor,
									Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3));
		}

		public <T1, T2, T3, T4> void to(TupleConstructor4<T1, T2, T3, T4, T> constructor,
										Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4));
		}

		public <T1, T2, T3, T4, T5> void to(TupleConstructor5<T1, T2, T3, T4, T5, T> constructor,
											Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5));
		}

		public <T1, T2, T3, T4, T5, T6> void to(TupleConstructor6<T1, T2, T3, T4, T5, T6, T> constructor,
												Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5, Class<T6> dependency6) {
			to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5), Key.of(dependency6));
		}

		public void toInstance(T instance) {
			to(args -> instance, asList());
		}

		public void asSingleton() {
			java.lang.reflect.Constructor<T> constructor;
			try {
				constructor = (java.lang.reflect.Constructor<T>) key.getTypeT().getClass().getConstructor();
			} catch (NoSuchMethodException e) {
				throw new RuntimeException("no default constructor for " + key.getTypeT(), e);
			}
			to(args -> {
				try {
					return constructor.newInstance();
				} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException("failed to create instance of " + key.getTypeT(), e);
				}
			}, asList());
		}
	}

	protected final <T> BindingBuilder<T> bind(Key<T> key) {
		return new BindingBuilder<>(key, null);
	}

	protected final <T> BindingBuilder<T> bind(TypeT<T> type) {
		return bind(Key.of(type));
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
	public final Map<Key<?>, Set<Binding<?>>> getBindings() {
		return bindings;
	}

	@Override
	public final Map<Scope, Map<Key<?>, Set<Binding<?>>>> getScopeBindings() {
		return scopeBindings;
	}

	@Override
	public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
		return conflictResolvers;
	}
}
