package io.datakernel.util;

public interface Sliceable<T> {
	T slice();

	@SuppressWarnings("unchecked")
	static <T> T trySlice(T value) {
		if (value instanceof Sliceable) return ((Sliceable<T>) value).slice();
		return value;
	}
}
