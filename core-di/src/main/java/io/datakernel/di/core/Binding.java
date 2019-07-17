package io.datakernel.di.core;

import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.LocationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.function.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A binding is one of the main components of DataKernel DI.
 * It boils down to "introspectable function", since it only describes
 * a {@link Factory function} to create an instance of T from an array of objects and
 * an array of its {@link Dependency dependencies} in known terms.
 * <p>
 * Also it contains a set of {@link io.datakernel.di.module.AbstractModule binding-DSL-like} static factory methods
 * as well as some functional transformations for the ease of creating immutable binding modifications.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public final class Binding<T> {
	@FunctionalInterface
	public interface Factory<R> {
		R create(InstanceLocator locator);
	}

	private final Dependency[] dependencies;
	private final Factory<T> factory;

	@Nullable
	private LocationInfo location;

	public Binding(@NotNull Dependency[] dependencies, @NotNull Factory<T> factory) {
		this(dependencies, null, factory);
	}

	public Binding(@NotNull Dependency[] dependencies, @Nullable LocationInfo location, @NotNull Factory<T> factory) {
		this.dependencies = dependencies;
		this.factory = factory;
		this.location = location;
	}

	public static <T> Binding<T> to(Class<? extends T> key) {
		return Binding.to(impl -> impl, key);
	}

	public static <T> Binding<T> to(Key<? extends T> key) {
		return Binding.to(impl -> impl, key);
	}

	public static <T> Binding<T> toInstance(@NotNull T instance) {
		return Binding.to(() -> instance);
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

	@SuppressWarnings("Convert2MethodRef")
	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, @NotNull Dependency[] dependencies) {
		return new Binding<>(dependencies,
				dependencies.length == 0 ?
						locator -> constructor.create() :
						locator -> {
							Object[] dependencyInstances = new Object[dependencies.length];
							for (int i = 0; i < dependencies.length; i++) {
								dependencyInstances[i] = locator.getInstanceOrNull(dependencies[i].getKey());
							}
							return (R) constructor.create(dependencyInstances);
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
		return new Binding<>(new Dependency[]{},
				locator -> constructor.create()
		);
	}

	public static <T1, R> Binding<R> to(@NotNull Constructor1<T1, R> constructor,
			@NotNull Key<T1> dependency1) {
		return new Binding<>(new Dependency[]{Dependency.toKey(dependency1)},
				locator -> constructor.create(
						locator.getInstanceOrNull(dependency1))
		);
	}

	public static <T1, T2, R> Binding<R> to(@NotNull Constructor2<T1, T2, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
		return new Binding<>(new Dependency[]{Dependency.toKey(dependency1), Dependency.toKey(dependency2)},
				locator -> constructor.create(
						locator.getInstanceOrNull(dependency1),
						locator.getInstanceOrNull(dependency2))
		);
	}

	public static <T1, T2, T3, R> Binding<R> to(@NotNull Constructor3<T1, T2, T3, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
		return new Binding<>(new Dependency[]{Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3)},
				locator -> constructor.create(
						locator.getInstanceOrNull(dependency1),
						locator.getInstanceOrNull(dependency2),
						locator.getInstanceOrNull(dependency3))
		);
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(@NotNull Constructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
		return new Binding<>(new Dependency[]{Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4)},
				locator -> constructor.create(
						locator.getInstanceOrNull(dependency1),
						locator.getInstanceOrNull(dependency2),
						locator.getInstanceOrNull(dependency3),
						locator.getInstanceOrNull(dependency4))
		);
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(@NotNull Constructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
		return new Binding<>(new Dependency[]{Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4), Dependency.toKey(dependency5)},
				locator -> constructor.create(
						locator.getInstanceOrNull(dependency1),
						locator.getInstanceOrNull(dependency2),
						locator.getInstanceOrNull(dependency3),
						locator.getInstanceOrNull(dependency4),
						locator.getInstanceOrNull(dependency5))
		);
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
		return new Binding<>(
				new Dependency[]{Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4), Dependency.toKey(dependency5), Dependency.toKey(dependency6)},
				locator -> constructor.create(
						locator.getInstanceOrNull(dependency1),
						locator.getInstanceOrNull(dependency2),
						locator.getInstanceOrNull(dependency3),
						locator.getInstanceOrNull(dependency4),
						locator.getInstanceOrNull(dependency5),
						locator.getInstanceOrNull(dependency6))
		);
	}

	public Binding<T> at(@Nullable LocationInfo location) {
		this.location = location;
		return this;
	}

	public Binding<T> onInstance(@NotNull Consumer<? super T> consumer) {
		return onInstance((locator, instance) -> consumer.accept(instance));
	}

	public Binding<T> onInstance(@NotNull BiConsumer<InstanceLocator, ? super T> consumer) {
		return new Binding<>(dependencies, location,
				locator -> {
					T instance = factory.create(locator);
					consumer.accept(locator, instance);
					return instance;
				}
		);
	}

	public <R> Binding<R> mapInstance(@NotNull Function<? super T, ? extends R> fn) {
		return mapInstance((locator, instance) -> fn.apply(instance));
	}

	public <R> Binding<R> mapInstance(@NotNull BiFunction<InstanceLocator, ? super T, ? extends R> fn) {
		return new Binding<>(dependencies, location,
				locator -> {
					T instance = factory.create(locator);
					return instance != null ? fn.apply(locator, instance) : null;
				}
		);
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
	public <K> Binding<T> mapDependency(@NotNull Key<K> dependency, @NotNull Function<? super K, ? extends K> fn) {
		return new Binding<>(dependencies, location,
				locator -> factory.create(new InstanceLocator() {
					@Override
					@Nullable
					public <Q> Q getInstanceOrNull(@NotNull Key<Q> key) {
						Q instance = locator.getInstanceOrNull(key);
						return !key.equals(dependency) ? instance : (Q) fn.apply((K) instance);
					}
				})
		);
	}

	public Binding<T> addDependencies(@NotNull Class<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).map(Key::of).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public Binding<T> addDependencies(@NotNull Key<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public Binding<T> addDependencies(@NotNull Dependency... extraDependencies) {
		if (extraDependencies.length == 0) {
			return this;
		}
		Dependency[] newDependencies = Arrays.copyOf(this.dependencies, this.dependencies.length + extraDependencies.length);
		System.arraycopy(extraDependencies, 0, newDependencies, this.dependencies.length, extraDependencies.length);
		return new Binding<>(newDependencies, location, factory);
	}

	@NotNull
	public Dependency[] getDependencies() {
		return dependencies;
	}

	@NotNull
	public Factory<T> getFactory() {
		return factory;
	}

	@Nullable
	public LocationInfo getLocation() {
		return location;
	}

	public String getDisplayString() {
		return Arrays.stream(dependencies).map(Dependency::getDisplayString).collect(joining(", ", "[", "]"));
	}

	@Override
	public String toString() {
		return Arrays.toString(dependencies);
	}
}
