package io.global.todo;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.global.debug.ObjectDisplayRegistry;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.service.ContainerScope;

import java.util.Map;

import static io.global.Utils.cachedContent;
import static io.global.debug.ObjectDisplayRegistryUtils.*;
import static io.global.ot.OTUtils.REGISTRY;
import static java.util.stream.Collectors.joining;

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
		if (debugServlet != null) {
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet;
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry objectDisplayRegistry() {
		Key<SetValue<Boolean>> setValueKey = new Key<SetValue<Boolean>>() {};
		return ObjectDisplayRegistry.create()
				.withShortDisplay(setValueKey,
						($, p) -> (p.isEmpty() ? "not changed" : "") +
								(p.getPrev() == null ? "added" :
										p.getNext() == null ? "removed" :
												(!p.getNext() ? "un" : "") + "completed"))
				.withDisplay(new Key<MapOperation<String, Boolean>>() {},
						(r, p) -> {
							Map<String, SetValue<Boolean>> operations = p.getOperations();
							if (operations.isEmpty()) {
								return "<empty>";
							}
							return operations.entrySet().stream()
									.map(e -> '\'' + shortText(e.getKey().substring(23)) + "' " + r.getShortDisplay(setValueKey, e.getValue()))
									.collect(joining("\n"));
						},
						($, p) -> {
							Map<String, SetValue<Boolean>> operations = p.getOperations();
							if (operations.isEmpty()) {
								return "<empty>";
							}
							return operations.entrySet().stream()
									.filter(entry -> !entry.getValue().isEmpty())
									.map(GlobalTodoModule::longDisplay)
									.collect(joining("\n"));
						});
	}

	private static String longDisplay(Map.Entry<String, SetValue<Boolean>> entry) {
		String key = entry.getKey();
		SetValue<Boolean> setValue = entry.getValue();
		Boolean prev = setValue.getPrev();
		Boolean next = setValue.getNext();
		String prefix = "Item '" + text(key.substring(23)) + '\'';
		if (prev == null) {
			return prefix + "  has been added " + ts(Long.parseLong(key.substring(0, 13)));
		}
		prefix += " added " + ts(Long.parseLong(key.substring(0, 13))) + " has been ";
		return prefix + (next == null ? "removed" :
				(!next ? "un" : "") + "completed");
	}
}
