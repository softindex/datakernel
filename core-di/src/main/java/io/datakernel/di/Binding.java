package io.datakernel.di;

import io.datakernel.di.BindingInitializer.Initializer;
import io.datakernel.di.util.Constructors.Factory;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public final class Binding<T> {
	private final Dependency[] dependencies;
	private final Factory<T> factory;

	@Nullable
	private LocationInfo location;

	private Binding(Dependency[] dependencies, Factory<T> factory, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.factory = factory;
		this.location = location;
	}

	public static <T> Binding<T> of(Dependency[] dependencies, Factory<T> factory) {
		return new Binding<>(dependencies, factory, null);
	}

	public static <T> Binding<T> of(Dependency[] dependencies, Factory<T> factory, LocationInfo location) {
		return new Binding<>(dependencies, factory, location);
	}

	public static <T> Binding<T> of(Key<?>[] dependencies, Factory<T> factory) {
		return new Binding<>(Arrays.stream(dependencies).map(k -> new Dependency(k, true)).toArray(Dependency[]::new), factory, null);
	}

	public static <T> Binding<T> of(Key<?>[] dependencies, Factory<T> factory, LocationInfo location) {
		return new Binding<>(Arrays.stream(dependencies).map(k -> new Dependency(k, true)).toArray(Dependency[]::new), factory, location);
	}

	public static <T> Binding<T> constant(T instance) {
		return new Binding<>(new Dependency[0], $ -> instance, null);
	}

	public Binding<T> apply(BindingInitializer<T> bindingInitializer) {
		Dependency[] addedDependencies = bindingInitializer.getDependencies();
		Dependency[] combinedDependencies = new Dependency[this.dependencies.length + addedDependencies.length];
		System.arraycopy(this.dependencies, 0, combinedDependencies, 0, this.dependencies.length);
		System.arraycopy(addedDependencies, 0, combinedDependencies, this.dependencies.length, addedDependencies.length);

		Initializer<T> initializer = bindingInitializer.getInitializer();
		int depsLen = this.dependencies.length;

		return new Binding<>(combinedDependencies,
				args -> {
					T instance = factory.create(Arrays.copyOf(args, depsLen));
					initializer.apply(instance, Arrays.copyOfRange(args, depsLen, args.length));
					return instance;
				},
				location);
	}

	public void setLocation(@Nullable LocationInfo location) {
		this.location = location;
	}

	public Dependency[] getDependencies() {
		return dependencies;
	}

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
		return "" + location + " : " + Arrays.toString(dependencies) + "";
	}
}
