package io.global.todo;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;

import static io.global.Utils.cachedContent;
import static io.global.ot.OTUtils.REGISTRY;

public final class GlobalTodoModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(GlobalTodoContainer.class).in(ContainerScope.class);
		bind(CodecFactory.class).toInstance(REGISTRY);
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<MapOperation<String, Boolean>> todoListServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet,
			@Optional @Named("debug") AsyncServlet debugServlet
	) {
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/ot/list/*", sessionDecorator.serve(todoListServlet))
				.map("/static/*", cachedContent().serve(staticServlet))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
		if (debugServlet != null){
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet;
	}
}
