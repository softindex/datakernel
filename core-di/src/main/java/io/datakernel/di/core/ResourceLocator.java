package io.datakernel.di.core;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface ResourceLocator {
	@NotNull <T> T getInstance(@NotNull Key<T> key);

	@NotNull <T> T getInstance(@NotNull Class<T> type);

	@Nullable <T> T getInstanceOrNull(@NotNull Key<T> key);

	@Nullable <T> T getInstanceOrNull(@NotNull Class<T> type);

	<T> T getInstanceOr(@NotNull Key<T> key, T defaultValue);

	<T> T getInstanceOr(@NotNull Class<T> type, T defaultValue);
}
