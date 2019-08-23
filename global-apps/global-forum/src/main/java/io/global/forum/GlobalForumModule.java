package io.global.forum;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.global.common.SimKey;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalForumModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	private final String forumFolder;
	private final String forumRepoName;
	private final String postsRepoPrefix;

	public GlobalForumModule(String forumFolder, String forumRepoName, String postsRepoPrefix) {
		this.forumFolder = forumFolder;
		this.forumRepoName = forumRepoName;
		this.postsRepoPrefix = postsRepoPrefix;
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	MustacheFactory mustacheFactory() {
		return new DefaultMustacheFactory(new File("static/templates"));
	}

	// @Provides
	// ContainerHolder<ForumUserContainer> containerHolder(Eventloop eventloop, BiFunction<Eventloop, PrivKey, ForumUserContainer> containerFactory) {
	// 	return new ContainerHolder<>(eventloop, containerFactory);
	// }
	//
	// @Provides
	// StructuredCodec<ForumOTOperation> channelOpCodec() {
	// 	return FORUM_OP_CODEC;
	// }
	//
	// @Provides
	// StructuredCodec<MapOperation<Long, Post>> commentOpCodec() {
	// 	return POST_OP_CODEC;
	// }

	// TODO: provide servlet
	// @Provides
	// AsyncServlet servlet(StaticServlet staticServlet, ContainerHolder<ForumUserContainer> containerHolder, MustacheFactory mustacheFactory) {
	// 	return RoutingServlet.create()
	// 			.map("/myForum/*", OwnerServlet.create(containerHolder, mustacheFactory))
	// 			.map("/user/:pubKey/*", PublicServlet.create(containerHolder, mustacheFactory))
	// 			.map("/*", staticServlet);
	// }

	@Provides
	StaticServlet staticServlet(StaticLoader staticLoader) {
		return StaticServlet.create(staticLoader)
				.withMappingNotFoundTo("404.html")
				.withIndexHtml();
	}

	@Provides
	StaticLoader staticLoader(Executor executor) {
		return StaticLoader.ofPath(executor, Paths.get("static/files"))
				.map(resource -> "no-thumbnail".equals(resource) ? "no_thumbnail.png" : resource);
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("app.http")));
	}

	@Provides
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	// @Provides
	// BiFunction<Eventloop, PrivKey, ForumUserContainer> factory(Config config, OTDriver otDriver, GlobalFsDriver fsDriver,
	// 		StructuredCodec<ForumOTOperation> channelOpCodec, StructuredCodec<MapOperation<Long, Post>> commentOpCodec) {
	// 	return (eventloop, privKey) -> ForumUserContainer.create(eventloop, privKey, otDriver, fsDriver, forumFolder,
	// 			forumRepoName, postsRepoPrefix, channelOpCodec, commentOpCodec);
	// }

}
