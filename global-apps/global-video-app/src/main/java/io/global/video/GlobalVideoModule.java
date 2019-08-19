package io.global.video;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.fs.local.GlobalFsDriver;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerHolder;
import io.global.video.container.VideoUserContainer;
import io.global.video.ot.channel.ChannelOTOperation;
import io.global.video.pojo.Comment;
import io.global.video.servlet.OwnerServlet;
import io.global.video.servlet.PublicServlet;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.video.Utils.CHANNEL_OP_CODEC;
import static io.global.video.Utils.COMMENT_OP_CODEC;

public final class GlobalVideoModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	private final String videosFolder;
	private final String videosRepoName;
	private final String commentsRepoPrefix;

	public GlobalVideoModule(String videosFolder, String videosRepoName, String commentsRepoPrefix) {
		this.videosFolder = videosFolder;
		this.videosRepoName = videosRepoName;
		this.commentsRepoPrefix = commentsRepoPrefix;
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	MustacheFactory mustacheFactory() {
		return new DefaultMustacheFactory(new File("static/templates"));
	}

	@Provides
	ContainerHolder<VideoUserContainer> containerHolder(Eventloop eventloop, BiFunction<Eventloop, PrivKey, VideoUserContainer> containerFactory) {
		return new ContainerHolder<>(eventloop, containerFactory);
	}

	@Provides
	StructuredCodec<ChannelOTOperation> channelOpCodec() {
		return CHANNEL_OP_CODEC;
	}

	@Provides
	StructuredCodec<MapOperation<Long, Comment>> commentOpCodec() {
		return COMMENT_OP_CODEC;
	}

	@Provides
	AsyncServlet servlet(StaticServlet staticServlet, ContainerHolder<VideoUserContainer> containerHolder, MustacheFactory mustacheFactory) {
		return RoutingServlet.create()
				.map("/myChannel/*", OwnerServlet.create(containerHolder, mustacheFactory))
				.map("/user/:pubKey/*", PublicServlet.create(containerHolder, mustacheFactory))
				.map("/*", staticServlet);
	}

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

	@Provides
	BiFunction<Eventloop, PrivKey, VideoUserContainer> factory(Config config, OTDriver otDriver, GlobalFsDriver fsDriver,
			StructuredCodec<ChannelOTOperation> channelOpCodec, StructuredCodec<MapOperation<Long, Comment>> commentOpCodec) {
		return (eventloop, privKey) -> VideoUserContainer.create(eventloop, privKey, otDriver, fsDriver, videosFolder,
				videosRepoName, commentsRepoPrefix, channelOpCodec, commentOpCodec);
	}

}
