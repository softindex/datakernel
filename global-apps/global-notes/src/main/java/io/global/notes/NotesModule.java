package io.global.notes;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.global.common.SimKey;
import io.global.notes.note.operation.EditOperation;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.dictionary.DictionaryOperation;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.notes.Utils.EDIT_OPERATION_CODEC;
import static io.global.notes.note.NoteOTSystem.createOTSystem;

public final class NotesModule extends AbstractModule {
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	private static final String RESOURCES_PATH = "front/build";

	private final String notesPrefixRepo;

	public NotesModule(String notesPrefixRepo) {
		this.notesPrefixRepo = notesPrefixRepo;
	}

	@Provides
	@Named("Notes")
	AsyncHttpServer server(Eventloop eventloop, RoutingServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	RoutingServlet mainServlet(
			DynamicOTNodeServlet<DictionaryOperation> notesServlet,
			DynamicOTNodeServlet<EditOperation> noteServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/notes/*", notesServlet)
				.map("/ot/note/:suffix/*", noteServlet)
				.map("/*", staticServlet);
	}

	@Provides
	DynamicOTNodeServlet<EditOperation> noteServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), EDIT_OPERATION_CODEC, notesPrefixRepo);
	}

	@Provides
	StaticServlet staticServlet(Eventloop eventloop, Executor executor) {
		Path staticDir = Paths.get(RESOURCES_PATH);
		StaticLoader resourceLoader = StaticLoader.ofPath(executor, staticDir);
		return StaticServlet.create(resourceLoader)
				.withMappingNotFoundTo("index.html");
	}

	@Provides
	OTDriver driver(Eventloop eventloop, GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config.getChild("executor"));
	}

}
