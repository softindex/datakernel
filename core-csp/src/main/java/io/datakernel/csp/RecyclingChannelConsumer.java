package io.datakernel.csp;

import io.datakernel.async.Promise;
import io.datakernel.util.Recyclable;
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
	public void close(@NotNull Throwable e) {
	}
}
