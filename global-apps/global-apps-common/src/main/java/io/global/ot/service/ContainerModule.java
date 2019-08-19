package io.global.ot.service;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.RoutingServlet;
import io.global.common.PrivKey;

import java.util.function.BiFunction;

public abstract class ContainerModule<T extends UserContainer> extends AbstractModule {

	@Provides
	ContainerHolder<T> containerHolder(Eventloop eventloop, BiFunction<Eventloop, PrivKey, T> containerFactory) {
		return new ContainerHolder<>(eventloop, containerFactory);
	}

	@Provides
	ServiceEnsuringServlet serviceEnsuringServlet(ContainerHolder<T> containerHolder, RoutingServlet servlet) {
		return ServiceEnsuringServlet.create(containerHolder, servlet);
	}
}
