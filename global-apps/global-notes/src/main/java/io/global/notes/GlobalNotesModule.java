package io.global.notes;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.global.ot.DynamicOTGraphServlet;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.edit.EditOTSystem;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOTStateListenerProxy;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;

import java.util.Objects;

import static io.global.Utils.cachedContent;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.OTUtils.createOTRegistry;

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
			DynamicOTGraphServlet<EditOperation> graphServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/notes/*", notesServlet)
						.map("/note/:suffix/*", noteServlet)
						.map("/graph/:suffix", graphServlet)))
				.map("/static/*", cachedContent().serve(staticServlet))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
	}

	@Provides
	DynamicOTGraphServlet<EditOperation> noteGraphServlet(DynamicOTUplinkServlet<EditOperation> noteServlet) {
		return noteServlet.createGraphServlet(Objects::toString);
	}

	@Provides
	@ContainerScope
	OTState<MapOperation<String, String>> notesState() {
		return new MapOTStateListenerProxy<>();
	}

}
