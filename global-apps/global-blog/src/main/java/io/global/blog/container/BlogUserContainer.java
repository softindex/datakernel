package io.global.blog.container;

import io.datakernel.common.reflection.TypeT;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.blog.dao.BlogDao;
import io.global.blog.dao.BlogDaoImpl;
import io.global.blog.ot.BlogMetadata;
import io.global.comm.container.CommGlobalState;
import io.global.comm.container.CommRepoNames;
import io.global.comm.pojo.UserId;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.kv.api.KvClient;
import io.global.ot.api.CommitId;
import io.global.ot.client.OTDriver;
import io.global.ot.service.UserContainer;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import io.global.ot.value.ChangeValueOTSystem;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.global.blog.util.Utils.REGISTRY;

public final class BlogUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(BlogUserContainer.class);

	private final Eventloop eventloop;
	private final KeyPair keys;

	private final OTStateManager<CommitId, ChangeValue<BlogMetadata>> metadataStateManager;

	private final CommGlobalState comm;
	private final BlogDao blogDao;

	private BlogUserContainer(Eventloop eventloop, OTDriver otDriver, KvClient<String, UserId> kvClient, FsClient fsClient, KeyPair keys, CommRepoNames names) {
		this.eventloop = eventloop;
		this.keys = keys;

		this.comm = CommGlobalState.create(eventloop, keys.getPrivKey(), otDriver, kvClient, fsClient, names);
		this.metadataStateManager = comm.createStateManager(names.getMetadata(), REGISTRY.get(new TypeT<ChangeValue<BlogMetadata>>() {}), ChangeValueOTSystem.get(), ChangeValueContainer.of(BlogMetadata.EMPTY));
		this.blogDao = new BlogDaoImpl(this);
	}

	public static BlogUserContainer create(Eventloop eventloop, PrivKey privKey, OTDriver otDriver, KvClient<String, UserId> kvClient, FsClient fsClient, CommRepoNames names) {
		return new BlogUserContainer(eventloop, otDriver, kvClient, fsClient, privKey.computeKeys(), names);
	}

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		return comm.start()
				.then($ -> metadataStateManager.start())
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		return comm.stop()
				.then($ -> metadataStateManager.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	public OTStateManager<CommitId, ChangeValue<BlogMetadata>> getMetadataStateManager() {
		return metadataStateManager;
	}

	public BlogDao getBlogDao() {
		return blogDao;
	}

	public CommGlobalState getComm() {
		return comm;
	}

	@Override
	public KeyPair getKeys() {
		return keys;
	}
}
