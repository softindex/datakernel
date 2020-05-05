package io.datakernel.csp;

import io.datakernel.common.api.Recyclable;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class RecyclingChannelConsumer<T extends Recyclable> implements ChannelConsumer<T> {
	@Override
	public @NotNull Promise<Void> accept(@Nullable Recyclable value) {
		if (value != null) {
			value.recycle();
		}
		return Promise.complete();
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
	}
}
