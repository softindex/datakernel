package io.global.ot.service;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.RoutingServlet;
import io.global.common.PrivKey;

import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public abstract class ContainerModule<T extends UserContainer> extends AbstractModule {

	@Provides
	ContainerKeyManager containerKeyManager(Executor executor, Path containersPath) {
		return FsContainerKeyManager.create(executor, containersPath, true);
	}

	@Provides
	ContainerManager<T> containerHolder(Eventloop eventloop, ContainerKeyManager containerKeyManager, BiFunction<Eventloop, PrivKey, T> containerFactory) {
		return ContainerManager.create(eventloop, containerKeyManager, containerFactory);
	}

	@Provides
	ServiceEnsuringServlet serviceEnsuringServlet(ContainerManager<T> containerManager, RoutingServlet servlet) {
		return ServiceEnsuringServlet.create(containerManager, servlet);
	}
}
