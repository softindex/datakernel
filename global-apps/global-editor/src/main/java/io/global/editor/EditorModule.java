package io.global.editor;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.ot.OTSystem;
import io.global.common.SimKey;
import io.global.editor.document.DocumentMultiOperation;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.service.ServiceEnsuringServlet;
import io.global.ot.shared.SharedReposOperation;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.editor.Utils.DOCUMENT_MULTI_OPERATION_CODEC;
import static io.global.editor.Utils.createMergedOTSystem;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class EditorModule extends AbstractModule {
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final String RESOURCES_PATH = "front/build";

	@Override
	protected void configure() {
		bind(new Key<OTSystem<DocumentMultiOperation>>() {}).toInstance(createMergedOTSystem());
		bind(new Key<StructuredCodec<DocumentMultiOperation>>() {}).toInstance(DOCUMENT_MULTI_OPERATION_CODEC);
		super.configure();
	}

	@Provides
	@Named("Editor")
	AsyncHttpServer provideServer(Eventloop eventloop, ServiceEnsuringServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	RoutingServlet provideMainServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> documentListServlet,
			DynamicOTNodeServlet<DocumentMultiOperation> documentServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/contacts/*", contactsServlet)
				.map("/ot/documents/*", documentListServlet)
				.map("/ot/document/:suffix/*", documentServlet)
				.map("/*", staticServlet);
	}

	@Provides
	StaticServlet provideStaticServlet(Eventloop eventloop, Executor executor) {
		Path staticDir = Paths.get(RESOURCES_PATH);
		StaticLoader resourceLoader = StaticLoader.ofPath(executor, staticDir);
		return StaticServlet.create(resourceLoader)
				.withMappingNotFoundTo("index.html");
	}

	@Provides
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor provideExecutor(Config config) {
		return getExecutor(config.getChild("executor"));
	}

}
