package io.datakernel.inspector;

import org.jetbrains.annotations.Nullable;

public interface BaseInspector<I extends BaseInspector<I>> {
	@Nullable
	<T extends I> T lookup(Class<T> type);

	static <I extends BaseInspector<I>, T extends I> T lookup(@Nullable BaseInspector<I> inspector, Class<T> type) {
		return inspector != null ? inspector.lookup(type) : null;
	}
}
