package io.datakernel.di.core;

import io.datakernel.di.impl.*;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.LocationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.checkArgument;
import static io.datakernel.di.util.Utils.union;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A binding is one of the main components of DataKernel DI.
 * It boils down to "introspectable function", since it only describes
 * a {@link BindingCompiler function} to create an instance of T from an array of objects and
 * an array of its {@link Dependency dependencies} in known terms.
 * <p>
 * Also it contains a set of {@link io.datakernel.di.module.AbstractModule binding-DSL-like} static factory methods
 * as well as some functional transformations for the ease of creating immutable binding modifications.
 */
@SuppressWarnings({"unused", "WeakerAccess", "ArraysAsListWithZeroOrOneArgument"})
public final class Binding<T> {
	private final Set<Dependency> dependencies;
	private final BindingCompiler<T> compiler;

	@Nullable
	private LocationInfo location;

	public Binding(@NotNull Set<Dependency> dependencies, @NotNull BindingCompiler<T> compiler) {
		this(dependencies, null, compiler);
	}

	public Binding(@NotNull Set<Dependency> dependencies, @Nullable LocationInfo location, @NotNull BindingCompiler<T> compiler) {
		this.dependencies = dependencies;
		this.compiler = compiler;
		this.location = location;
	}

	public static <T> Binding<T> to(Class<? extends T> key) {
		return Binding.to(Key.of(key));
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> to(Key<? extends T> key) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(key))),
				(compiledBindings, level, index) -> (CompiledBinding<T>) compiledBindings.locate(key));
	}

	public static <T> Binding<T> toInstance(@NotNull T instance) {
		return new Binding<>(emptySet(),
				(compiledBindings, level, index) ->
						new CompiledBinding<T>() {
							@Override
							public T getInstance(AtomicReferenceArray[] instances, int lockedLevel) {
								//noinspection unchecked
								instances[level].lazySet(index, instance);
								return instance;
							}

							@Override
							public T createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
								return instance;
							}
						});
	}

	public static <T> Binding<T> toSupplier(@NotNull Key<? extends Supplier<? extends T>> supplierKey) {
		return Binding.to(Supplier::get, supplierKey);
	}

	public static <T> Binding<T> toSupplier(@NotNull Class<? extends Supplier<? extends T>> supplierType) {
		return Binding.to(Supplier::get, supplierType);
	}

	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, @NotNull Class<?>[] types) {
		return Binding.to(constructor, Stream.of(types).map(Key::of).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, @NotNull Key<?>[] keys) {
		return Binding.to(constructor, Stream.of(keys).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, @NotNull Dependency[] dependencies) {
		if (dependencies.length == 0) {
			return to(constructor::create);
		}
		return new Binding<>(new HashSet<>(asList(dependencies)),
				(compiledBindings, level, index) -> {
					CompiledBinding<?>[] bindings = new CompiledBinding[dependencies.length];
					for (int i = 0; i < dependencies.length; i++) {
						bindings[i] = compiledBindings.locate(dependencies[i].getKey());
					}
					return level == 0 ?
							new AbstractRootCompiledBinding<R>(level, index) {
								@Nullable
								@Override
								public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
									Object[] args = new Object[bindings.length];
									for (int i = 0; i < bindings.length; i++) {
										args[i] = bindings[i].getInstance(instances, lockedLevel);
									}
									return constructor.create(args);
								}
							} :
							new AbstractCompiledBinding<R>(level, index) {
								@Nullable
								@Override
								public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
									Object[] args = new Object[bindings.length];
									for (int i = 0; i < bindings.length; i++) {
										args[i] = bindings[i].getInstance(instances, lockedLevel);
									}
									return constructor.create(args);
								}
							};
				});
	}

	public static <T1, R> Binding<R> to(@NotNull Constructor1<T1, R> constructor,
			@NotNull Class<T1> dependency1) {
		return Binding.to(constructor, Key.of(dependency1));
	}

	public static <T1, T2, R> Binding<R> to(@NotNull Constructor2<T1, T2, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2));
	}

	public static <T1, T2, T3, R> Binding<R> to(@NotNull Constructor3<T1, T2, T3, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3));
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(@NotNull Constructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4));
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(@NotNull Constructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5));
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5, @NotNull Class<T6> dependency6) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5), Key.of(dependency6));
	}

	public static <R> Binding<R> to(@NotNull Constructor0<R> constructor) {
		return new Binding<>(emptySet(),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create();
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									@Override
									public R createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return doCreateInstance(instances, lockedLevel);
									}

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create();
									}
								});
	}

	public static <T1, R> Binding<R> to(@NotNull Constructor1<T1, R> constructor,
			@NotNull Key<T1> dependency1) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(dependency1))),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel));
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);

									@Override
									public R createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return doCreateInstance(instances, lockedLevel);
									}

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel));
									}
								});
	}

	public static <T1, T2, R> Binding<R> to(@NotNull Constructor2<T1, T2, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2))),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel));
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel));
									}
								});
	}

	public static <T1, T2, T3, R> Binding<R> to(@NotNull Constructor3<T1, T2, T3, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3))),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel));
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel));
									}
								});
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(@NotNull Constructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4))),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);
									final CompiledBinding<T4> binding4 = compiledBindings.locate(dependency4);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel),
												binding4.getInstance(instances, lockedLevel));
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);
									final CompiledBinding<T4> binding4 = compiledBindings.locate(dependency4);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel),
												binding4.getInstance(instances, lockedLevel));
									}
								});
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(@NotNull Constructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4), Dependency.toKey(dependency5))),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);
									final CompiledBinding<T4> binding4 = compiledBindings.locate(dependency4);
									final CompiledBinding<T5> binding5 = compiledBindings.locate(dependency5);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel),
												binding4.getInstance(instances, lockedLevel),
												binding5.getInstance(instances, lockedLevel));
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);
									final CompiledBinding<T4> binding4 = compiledBindings.locate(dependency4);
									final CompiledBinding<T5> binding5 = compiledBindings.locate(dependency5);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel),
												binding4.getInstance(instances, lockedLevel),
												binding5.getInstance(instances, lockedLevel));
									}
								});
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
		return new Binding<>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4), Dependency.toKey(dependency5), Dependency.toKey(dependency6))),
				(compiledBindings, level, index) ->
						level == 0 ?
								new AbstractRootCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);
									final CompiledBinding<T4> binding4 = compiledBindings.locate(dependency4);
									final CompiledBinding<T5> binding5 = compiledBindings.locate(dependency5);
									final CompiledBinding<T6> binding6 = compiledBindings.locate(dependency6);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel),
												binding4.getInstance(instances, lockedLevel),
												binding5.getInstance(instances, lockedLevel),
												binding6.getInstance(instances, lockedLevel));
									}
								} :
								new AbstractCompiledBinding<R>(level, index) {
									final CompiledBinding<T1> binding1 = compiledBindings.locate(dependency1);
									final CompiledBinding<T2> binding2 = compiledBindings.locate(dependency2);
									final CompiledBinding<T3> binding3 = compiledBindings.locate(dependency3);
									final CompiledBinding<T4> binding4 = compiledBindings.locate(dependency4);
									final CompiledBinding<T5> binding5 = compiledBindings.locate(dependency5);
									final CompiledBinding<T6> binding6 = compiledBindings.locate(dependency6);

									@Nullable
									@Override
									public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										return constructor.create(
												binding1.getInstance(instances, lockedLevel),
												binding2.getInstance(instances, lockedLevel),
												binding3.getInstance(instances, lockedLevel),
												binding4.getInstance(instances, lockedLevel),
												binding5.getInstance(instances, lockedLevel),
												binding6.getInstance(instances, lockedLevel));
									}
								});
	}

	public Binding<T> at(@Nullable LocationInfo location) {
		this.location = location;
		return this;
	}

	public Binding<T> onInstance(@NotNull Consumer<? super T> consumer) {
		return mapInstance(null, (args, instance) -> {
			consumer.accept(instance);
			return instance;
		});
	}

	public <R> Binding<R> mapInstance(@NotNull Function<? super T, ? extends R> fn) {
		return mapInstance(null, (args, instance) -> fn.apply(instance));
	}

	public <R> Binding<R> mapInstance(@Nullable List<Key<?>> keys, @NotNull BiFunction<Object[], ? super T, ? extends R> fn) {
		if (keys != null) {
			checkArgument(dependencies.stream().map(Dependency::getKey).collect(toSet()).containsAll(new HashSet<>(keys)));
		}
		return new Binding<>(dependencies, location,
				(compiledBindings, level, index) ->
						new AbstractCompiledBinding<R>(level, index) {
							final CompiledBinding<T> originalBinding = compiler.compileForCreateOnly(compiledBindings, level, index);
							final CompiledBinding[] bindings =
									keys == null ?
											null :
											keys.stream().map(compiledBindings::locate).toArray(CompiledBinding[]::new);

							@Nullable
							@Override
							public R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
								Object[] args = null;
								if (bindings != null) {
									args = new Object[bindings.length];
									for (int i = 0; i < bindings.length; i++) {
										args[i] = bindings[i].getInstance(instances, lockedLevel);
									}
								}
								T instance = originalBinding.createInstance(instances, lockedLevel);
								return instance != null ? fn.apply(args, instance) : null;
							}
						});
	}

	public <K> Binding<T> onDependency(@NotNull Class<K> dependency, @NotNull Consumer<? super K> consumer) {
		return onDependency(Key.of(dependency), consumer);
	}

	public <K> Binding<T> onDependency(@NotNull Key<K> dependency, @NotNull Consumer<? super K> consumer) {
		return mapDependency(dependency, v -> {
			consumer.accept(v);
			return v;
		});
	}

	public <K> Binding<T> mapDependency(@NotNull Class<K> dependency, @NotNull Function<? super K, ? extends K> fn) {
		return mapDependency(Key.of(dependency), fn);
	}

	@SuppressWarnings("unchecked")
	public <K> Binding<T> mapDependency(@NotNull Key<K> key, @NotNull Function<? super K, ? extends K> fn) {
		return new Binding<>(dependencies, location,
				(compiledBindings, level, index) ->
						compiler.compile(new CompiledBindingLocator() {
							@Override
							public @NotNull <Q> CompiledBinding<Q> locate(Key<Q> k) {
								CompiledBinding<Q> originalBinding = compiledBindings.locate(k);
								if (!k.equals(key)) return originalBinding;
								return new CompiledBinding<Q>() {
									@Nullable
									@Override
									public Q getInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										Q instance = originalBinding.getInstance(instances, lockedLevel);
										return (Q) fn.apply((K) instance);
									}

									@Nullable
									@Override
									public Q createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										Q instance = originalBinding.createInstance(instances, lockedLevel);
										return (Q) fn.apply((K) instance);
									}
								};
							}
						}, level, index));
	}

	public Binding<T> addDependencies(@NotNull Class<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).map(Key::of).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public Binding<T> addDependencies(@NotNull Key<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public Binding<T> addDependencies(@NotNull Dependency... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).collect(toSet()));
	}

	public Binding<T> addDependencies(@NotNull Set<Dependency> extraDependencies) {
		return extraDependencies.isEmpty() ?
				this :
				new Binding<>(union(dependencies, extraDependencies), location, compiler);
	}

	public Binding<T> initializeWith(BindingInitializer<T> bindingInitializer) {
		return bindingInitializer == BindingInitializer.noop() ?
				this :
				new Binding<>(union(dependencies, bindingInitializer.getDependencies()),
						(compiledBindings, level, index) ->
								new AbstractCompiledBinding<T>(level, index) {
									final CompiledBinding<T> compiledBinding = compiler.compileForCreateOnly(compiledBindings, level, index);
									final CompiledBindingInitializer<T> consumer = bindingInitializer.getCompiler().compile(compiledBindings);

									@Override
									public T doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel) {
										T instance = compiledBinding.createInstance(instances, lockedLevel);
										consumer.initInstance(instance, instances, lockedLevel);
										return instance;
									}
								});
	}

	@NotNull
	public Set<Dependency> getDependencies() {
		return dependencies;
	}

	@NotNull
	public BindingCompiler<T> getCompiler() {
		return compiler;
	}

	@Nullable
	public LocationInfo getLocation() {
		return location;
	}

	public String getDisplayString() {
		return dependencies.stream().map(Dependency::getDisplayString).collect(joining(", ", "[", "]"));
	}

	@Override
	public String toString() {
		return dependencies.toString();
	}
}
