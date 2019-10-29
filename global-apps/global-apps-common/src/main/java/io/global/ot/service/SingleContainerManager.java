package io.global.ot.service;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SingleContainerManager<C extends UserContainer> implements ContainerManager<C> {
	private final C container;

	private SingleContainerManager(C container) {
		this.container = container;
	}

	public static <C extends UserContainer> SingleContainerManager<C> of(C container) {
		return new SingleContainerManager<>(container);
	}

	@Nullable
	@Override
	public C getUserContainer(String id) {
		return container;
	}

	@Override
	public boolean isSingleMode() {
		return true;
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return container.getEventloop();
	}

	@Override
	public @NotNull Promise<?> start() {
		return container.start();
	}

	@Override
	public @NotNull Promise<?> stop() {
		return container.stop();
	}
}
