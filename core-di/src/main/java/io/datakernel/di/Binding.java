package io.datakernel.di;

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public final class Binding<T> {
	public interface Factory<T> {
		T create(Object[] args);

		static <T> Factory<T> of(Function<Object[], T> creator) {
			return new Factory<T>() {
				@Override
				public T create(Object[] x) {
					return creator.apply(x);
				}

				@Override
				public void lateinit(T instance, Object[] args) {
				}
			};
		}

		void lateinit(T instance, Object[] args);
	}

	private final Key<T> key;
	private final Dependency[] dependencies;
	private final Factory<T> factory;

	@Nullable
	private LocationInfo location;

	private Binding(Key<T> key, Dependency[] dependencies, Factory<T> factory, @Nullable LocationInfo location) {
		this.key = key;
		this.dependencies = dependencies;
		this.factory = factory;
		this.location = location;
	}

	public static <T> Binding<T> of(Key<T> key, Dependency[] dependencies, Factory<T> factory) {
		return new Binding<>(key, dependencies, factory, null);
	}

	public static <T> Binding<T> of(Key<T> key, Dependency[] dependencies, Function<Object[], T> factory) {
		return new Binding<>(key, dependencies, Factory.of(factory), null);
	}

	public static <T> Binding<T> of(Key<T> key, Dependency[] dependencies, Factory<T> factory, LocationInfo location) {
		return new Binding<>(key, dependencies, factory, location);
	}

	public static <T> Binding<T> of(Key<T> key, Dependency[] dependencies, Function<Object[], T> factory, LocationInfo location) {
		return new Binding<>(key, dependencies, Factory.of(factory), location);
	}

	public void setLocation(@Nullable LocationInfo location) {
		this.location = location;
	}

	public Key<T> getKey() {
		return key;
	}

	public Dependency[] getDependencies() {
		return dependencies;
	}

	public Binding.Factory<T> getFactory() {
		return factory;
	}

	@Nullable
	public LocationInfo getLocation() {
		return location;
	}

	public String getDisplayString() {
		return key.getDisplayString() + " -> " + Arrays.stream(dependencies).map(Dependency::getDisplayString).collect(joining(", ", "[", "]"));
	}

	@Override
	public String toString() {
		return "Binding{" + key + ", dependencies=" + Arrays.toString(dependencies) + '}';
	}
}
