package io.datakernel.di.core;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is an interface for objects that can store and retrieve instances by some {@link Key keys}.
 * It is used as an argument in binding {@link io.datakernel.di.core.Binding.Factory factories}.
 * <p>
 * A base implementation of such object is {@link Injector the injector} itself, and then it can be proxied
 * to add additional behaviour, such as filtering or modifying the result.
 */
public interface InstanceLocator {
	/**
	 * Returns an instance associated with a given key, or <code>null</code>
	 * if such instance is not possible to create/retrieve.
	 */
	@Nullable <T> T getInstanceOrNull(@NotNull Key<T> key);

	/**
	 * @see #getInstanceOrNull(Key)
	 */
	default @Nullable <T> T getInstanceOrNull(@NotNull Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	/**
	 * Same as {@link #getInstanceOrNull(Key)}, but throws an exception when no instance was found.
	 * @throws DIException when instance cannot be constructed/retrieved.
	 */
	default @NotNull <T> T getInstance(@NotNull Key<T> key) {
		T instance = getInstanceOrNull(key);
		if (instance != null) {
			return instance;
		}
		throw DIException.cannotConstruct(key, null);
	}

	/**
	 * @see #getInstance(Key)
	 */
	default @NotNull <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.ofType(type));
	}

	/**
	 * Same as {@link #getInstanceOrNull(Key)}, but replaces <code>null</code> with given default value.
	 */
	default <T> T getInstanceOr(@NotNull Key<T> key, T defaultValue) {
		T instance = getInstanceOrNull(key);
		return instance != null ? instance : defaultValue;
	}

	/**
	 * @see #getInstanceOr(Key, Object)
	 */
	default <T> T getInstanceOr(@NotNull Class<T> type, T defaultValue) {
		return getInstanceOr(Key.of(type), defaultValue);
	}
}
