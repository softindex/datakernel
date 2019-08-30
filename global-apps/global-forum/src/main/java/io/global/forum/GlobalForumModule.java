package io.global.forum;

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.forum.container.ForumRepoNames;
import io.global.forum.container.ForumUserContainer;
import io.global.fs.local.GlobalFsDriver;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerKeyManager;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.FsContainerKeyManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalForumModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	private final String forumFsDir;
	private final ForumRepoNames forumRepoNames;

	public GlobalForumModule(String forumFsDir, ForumRepoNames forumRepoNames) {
		this.forumFsDir = forumFsDir;
		this.forumRepoNames = forumRepoNames;
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(ContainerManager<ForumUserContainer> containerManager, @Named("Forum") AsyncServlet forumServlet) {
		return RoutingServlet.create()
				.map("/:pubKey/*", request -> {
					try {
						PubKey pubKey = PubKey.fromString(request.getPathParameter("pubKey"));
						ForumUserContainer container = containerManager.getUserContainer(pubKey);
						if (container == null) {
							return Promise.of(HttpResponse.notFound404());
						}
						request.attach(container);
						return forumServlet.serve(request);
					} catch (ParseException ignored) {
						return Promise.of(HttpResponse.notFound404());
					}
				});
	}

	@Provides
	@Named("Forum")
	AsyncServlet forumServlet() {
		return request -> {
			ForumUserContainer userContainer = request.getAttachment(ForumUserContainer.class);
			PubKey pubKey = userContainer.getKeys().getPubKey();
			return Promise.of(HttpResponse.ok200().withHtml("<h1>Container exists</h1><h2>" + pubKey.asString() + "</h2>"));
		};
	}

	@Provides
	ContainerKeyManager containerKeyManager(Executor executor, Config config) {
		Path containersDir = config.get(ofPath(), "containers.dir", Paths.get("containers"));
		return FsContainerKeyManager.create(executor, containersDir, true);
	}

	@Provides
	ContainerManager<ForumUserContainer> containerHolder(Eventloop eventloop, ContainerKeyManager containerKeyManager, BiFunction<Eventloop, PrivKey, ForumUserContainer> factory) {
		return ContainerManager.create(eventloop, containerKeyManager, factory);
	}

	@Provides
	BiFunction<Eventloop, PrivKey, ForumUserContainer> containerFactory(OTDriver otDriver, GlobalFsDriver fsDriver) {
		return (eventloop, privKey) ->
				ForumUserContainer.create(eventloop, privKey, otDriver, fsDriver.adapt(privKey).subfolder(forumFsDir), forumRepoNames);
	}

	@Provides
	OTDriver otDriver(GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

}
