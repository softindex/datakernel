package io.datakernel.di.core;

import io.datakernel.di.core.BindingInitializer.Initializer;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.LocationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.checkArgument;
import static java.util.stream.Collectors.joining;

public final class Binding<T> {
	private final Dependency[] dependencies;
	private final Factory<T> factory;

	@Nullable
	private LocationInfo location;

	private Binding(@NotNull Dependency[] dependencies, @NotNull Factory<T> factory, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.factory = factory;
		this.location = location;
	}

	public static <R> Binding<R> to(Factory<R> factory) {
		return Binding.to(factory, new Dependency[0]);
	}

	public static <R> Binding<R> to(@NotNull Factory<R> factory, @NotNull Class<?>[] dependencies) {
		return new Binding<>(Stream.of(dependencies).map(type -> new Dependency(Key.of(type), true)).toArray(Dependency[]::new),
				factory, null);
	}

	public static <R> Binding<R> to(@NotNull Factory<R> factory, @NotNull Key<?>[] dependencies) {
		return new Binding<>(Stream.of(dependencies).map(key -> new Dependency(key, true)).toArray(Dependency[]::new),
				factory, null);
	}

	public static <R> Binding<R> to(@NotNull Factory<R> factory, @NotNull Dependency[] dependencies) {
		return new Binding<>(dependencies,
				factory, null);
	}

	public static <T> Binding<T> to(Class<? extends T> key) {
		return Binding.to(impl -> impl, key);
	}

	public static <T> Binding<T> to(Key<? extends T> key) {
		return Binding.to(impl -> impl, key);
	}

	public static <T> Binding<T> toInstance(@NotNull T instance) {
		return Binding.to($ -> instance);
	}

	public static <R> Binding<R> to(Constructor0<R> constructor) {
		return Binding.to(constructor, new Dependency[0]);
	}

	public static <T1, R> Binding<R> to(Constructor1<T1, R> constructor,
			Class<T1> dependency1) {
		return Binding.to(constructor, new Class[]{dependency1});
	}

	public static <T1, T2, R> Binding<R> to(Constructor2<T1, T2, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2) {
		return Binding.to(constructor, new Class[]{dependency1, dependency2});
	}

	public static <T1, T2, T3, R> Binding<R> to(Constructor3<T1, T2, T3, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3) {
		return Binding.to(constructor, new Class[]{dependency1, dependency2, dependency3});
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(Constructor4<T1, T2, T3, T4, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4) {
		return Binding.to(constructor, new Class[]{dependency1, dependency2, dependency3, dependency4});
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(Constructor5<T1, T2, T3, T4, T5, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5) {
		return Binding.to(constructor, new Class[]{dependency1, dependency2, dependency3, dependency4, dependency5});
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5, Class<T6> dependency6) {
		return Binding.to(constructor, new Class[]{dependency1, dependency2, dependency3, dependency4, dependency5, dependency6});
	}

	public static <T1, R> Binding<R> to(Constructor1<T1, R> constructor,
			Key<T1> dependency1) {
		return Binding.to(constructor, new Key[]{dependency1});
	}

	public static <T1, T2, R> Binding<R> to(Constructor2<T1, T2, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2) {
		return Binding.to(constructor, new Key[]{dependency1, dependency2});
	}

	public static <T1, T2, T3, R> Binding<R> to(Constructor3<T1, T2, T3, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
		return Binding.to(constructor, new Key[]{dependency1, dependency2, dependency3});
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(Constructor4<T1, T2, T3, T4, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
		return Binding.to(constructor, new Key[]{dependency1, dependency2, dependency3, dependency4});
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(Constructor5<T1, T2, T3, T4, T5, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
		return Binding.to(constructor, new Key[]{dependency1, dependency2, dependency3, dependency4, dependency5});
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
		return Binding.to(constructor, new Key[]{dependency1, dependency2, dependency3, dependency4, dependency5, dependency6});
	}

	public Binding<T> at(@Nullable LocationInfo location) {
		this.location = location;
		return this;
	}

	public Binding<T> initialize(BindingInitializer<T> bindingInitializer) {
		if (bindingInitializer == BindingInitializer.noop()) {
			return this;
		}
		Dependency[] addedDependencies = bindingInitializer.getDependencies();
		Dependency[] combinedDependencies = new Dependency[this.dependencies.length + addedDependencies.length];
		System.arraycopy(this.dependencies, 0, combinedDependencies, 0, this.dependencies.length);
		System.arraycopy(addedDependencies, 0, combinedDependencies, this.dependencies.length, addedDependencies.length);

		Initializer<T> initializer = bindingInitializer.getInitializer();
		int depsLen = this.dependencies.length;

		return new Binding<>(combinedDependencies,
				args -> {
					T instance = factory.create(Arrays.copyOfRange(args, 0, depsLen));
					initializer.apply(instance, Arrays.copyOfRange(args, depsLen, args.length));
					return instance;
				},
				location);
	}

	public Binding<T> onInstance(@NotNull Consumer<? super T> consumer) {
		return new Binding<>(dependencies,
				args -> {
					T instance = factory.create(args);
					consumer.accept(instance);
					return instance;
				},
				location);
	}

	public <R> Binding<R> mapInstance(@NotNull Function<? super T, ? extends R> fn) {
		return new Binding<>(dependencies,
				args -> {
					T instance = factory.create(args);
					return instance != null ? fn.apply(instance) : null;
				},
				location);
	}

	public <R> Binding<R> mapInstance(@NotNull BiFunction<Object[], ? super T, ? extends R> fn) {
		return new Binding<>(dependencies,
				args -> {
					T instance = factory.create(args);
					return instance != null ? fn.apply(args, instance) : null;
				},
				location);
	}

	public Binding<T> onDependencies(@NotNull Consumer<Object[]> consumer) {
		return new Binding<>(dependencies,
				args -> {
					consumer.accept(args);
					return factory.create(args);
				},
				location);
	}

	public Binding<T> mapDependencies(@NotNull Function<Object[], Object[]> fn) {
		return new Binding<>(dependencies,
				args -> factory.create(fn.apply(args)),
				location);
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
		List<Integer> positionsList = new ArrayList<>();
		for (int i = 0; i < dependencies.length; i++) {
			if (dependencies[i].getKey().equals(dependency)) {
				positionsList.add(i);
			}
		}
		int[] positions = positionsList.stream().mapToInt(Integer::intValue).toArray();
		checkArgument(positions.length != 0);
		return new Binding<>(dependencies,
				args -> {
					K mapped = fn.apply((K) args[positions[0]]);
					for (int i = 1; i < positions.length; i++) {
						args[i] = mapped;
					}
					return factory.create(args);
				},
				location);
	}

	public Binding<T> addDependency(@NotNull Class<?> dependency) {
		return addDependency(Key.of(dependency));
	}

	public Binding<T> addDependency(@NotNull Key<?> dependency) {
		return addDependency(new Dependency(dependency, true));
	}

	public Binding<T> addDependency(@NotNull Dependency dependency) {
		Dependency[] newDependencies = Arrays.copyOf(this.dependencies, dependencies.length + 1);
		newDependencies[newDependencies.length - 1] = dependency;
		return new Binding<>(newDependencies, newArgs -> factory.create(Arrays.copyOf(newArgs, dependencies.length)), location);
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
