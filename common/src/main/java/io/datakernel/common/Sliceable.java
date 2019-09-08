package io.datakernel.common;

/**
 * Some objects (mainly {@link io.datakernel.bytebuf.ByteBuf ByteBufs}) have the reference counter
 * so that they can be 'lightly cloned' or 'sliced' and the {@link Recyclable#recycle() recycle} method should be called
 * on all 'light clones' or 'slices' for the object to be actually recycled.
 * This is used to share the ownership between multiple consumers.
 */
public interface Sliceable<T> {
	/**
	 * Creates a 'light clone' of this object.
	 * <p>
	 * This can return either 'this' with reference counter increased.
	 * Or a new wrapper around something that has its reference counter increased.
	 */
	T slice();

	/**
	 * If a given object is sliceable, return a slice, or else just return the object.
	 */
	@SuppressWarnings("unchecked")
	static <T> T trySlice(T value) {
		if (value instanceof Sliceable) return ((Sliceable<T>) value).slice();
		return value;
	}
}
