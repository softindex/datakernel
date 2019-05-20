package io.datakernel.di;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public final class Binding<T> {
	public interface Constructor<T> {
		T construct(Object[] args);
	}

	private final Key<T> key;
	private final Dependency[] dependencies;
	private final Constructor<T> constructor;

	public Binding(Key<T> key, Dependency[] dependencies, Constructor<T> constructor) {
		this.key = key;
		this.dependencies = dependencies;
		this.constructor = constructor;
	}

	public Key<T> getKey() {
		return key;
	}

	public Dependency[] getDependencies() {
		return dependencies;
	}

	public Constructor<T> getConstructor() {
		return constructor;
	}

	public String getDisplayString() {
		return key.getDisplayString() + " -> " + Arrays.stream(dependencies).map(Dependency::getDisplayString).collect(joining(", ", "[", "]"));
	}

	@Override
	public String toString() {
		return "Binding{" + key + ", dependencies=" + Arrays.toString(dependencies) + '}';
	}
}
