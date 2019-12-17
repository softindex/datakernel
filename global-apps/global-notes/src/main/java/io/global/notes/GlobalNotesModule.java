package io.global.notes;

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
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.global.debug.ObjectDisplayRegistry;
import io.global.debug.ObjectDisplayRegistryUtils;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.edit.EditOTSystem;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOTStateListenerProxy;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.service.ContainerScope;

import static io.global.Utils.cachedContent;
import static io.global.debug.ObjectDisplayRegistryUtils.*;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.OTUtils.createOTRegistry;
import static java.util.stream.Collectors.joining;

public final class GlobalNotesModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(CodecFactory.class).toInstance(createOTRegistry()
				.with(EditOperation.class, EDIT_OPERATION_CODEC));
		bind(GlobalNotesContainer.class).in(ContainerScope.class);
		bind(new Key<OTSystem<EditOperation>>() {}).toInstance(EditOTSystem.createOTSystem());
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<MapOperation<String, String>> notesServlet,
			DynamicOTUplinkServlet<EditOperation> noteServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet,
			@Optional @Named("debug") AsyncServlet debugServlet
	) {
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/notes/*", notesServlet)
						.map("/note/:suffix/*", noteServlet)))
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
	OTState<MapOperation<String, String>> notesState() {
		return new MapOTStateListenerProxy<>();
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry objectDisplayRegistry() {
		return ObjectDisplayRegistryUtils.forEditOperation()
				.withDisplay(new Key<MapOperation<String, String>>() {},
						($, p) -> {
							if (p.getOperations().isEmpty()) {
								return "<empty>";
							}
							return p.getOperations().values()
									.stream()
									.map(setValue -> {
										if (setValue.isEmpty()) {
											return "<empty>";
										}
										if (setValue.getPrev() == null) {
											assert setValue.getNext() != null;
											return "Created note '" + shortText(setValue.getNext()) + '\'';
										}
										if (setValue.getNext() == null) {
											return "Deleted note '" + shortText(setValue.getPrev()) + '\'';
										}
										return "Renamed note '" + shortText(setValue.getPrev()) + "' to '" + shortText(setValue.getNext()) + '\'';
									})
									.collect(joining("\n"));
						},
						($, p) -> {
							if (p.getOperations().isEmpty()) {
								return "<empty>";
							}
							return p.getOperations().entrySet()
									.stream()
									.map(e -> {
										String key = e.getKey();
										SetValue<String> setValue = e.getValue();
										if (setValue.isEmpty()) {
											return "Note" +
													(setValue.getPrev() != null ?
															(" '" + text(setValue.getPrev()) + '\'') :
															"") +
													" with id " + hashId(key) + " did not change";
										}
										if (setValue.getPrev() == null) {
											assert setValue.getNext() != null;
											return "Created note '" + text(setValue.getNext()) + "' with id " + hashId(key);
										}
										if (setValue.getNext() == null) {
											return "Deleted note '" + text(setValue.getPrev()) + "' with id" + hashId(key);
										}
										return "Renamed note with id " + hashId(key) + " from '" + text(setValue.getPrev()) + "' to '" + text(setValue.getNext());

									})
									.collect(joining("\n"));
						});
	}

}
