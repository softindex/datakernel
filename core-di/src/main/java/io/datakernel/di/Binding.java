package io.datakernel.di;

public final class Binding<T> {
	public interface Constructor<T> {
		T construct(Object[] args);
	}

	private final Key<T> key;
	private final Key<?>[] dependencies;
	private final Constructor<T> constructor;

	public Binding(Key<T> key, Key<?>[] dependencies, Constructor<T> constructor) {
		this.key = key;
		this.dependencies = dependencies;
		this.constructor = constructor;
	}

	public Key<T> getKey() {
		return key;
	}

	public Key<?>[] getDependencies() {
		return dependencies;
	}

	public Constructor<T> getConstructor() {
		return constructor;
	}
}
