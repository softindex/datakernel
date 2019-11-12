package io.global.ot.service;

import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.global.common.PrivKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SingleContainerManager<C extends UserContainer> implements ContainerManager<C> {
	private final Eventloop eventloop;
	private final C container;

	private SingleContainerManager(Eventloop eventloop, C container) {
		this.eventloop = eventloop;
		this.container = container;
	}

	public static <C extends UserContainer> SingleContainerManager<C> create(Injector injector, Key<C> containerKey, Eventloop eventloop, PrivKey privKey) {
		Injector subinjector = injector.enterScope(Scope.of(ContainerScope.class));
		subinjector.putInstance(PrivKey.class, privKey);
		return new SingleContainerManager<>(eventloop, subinjector.getInstance(containerKey));
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
		return eventloop;
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
