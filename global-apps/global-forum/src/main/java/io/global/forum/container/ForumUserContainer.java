package io.global.forum.container;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.comm.container.CommGlobalState;
import io.global.comm.container.CommRepoNames;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ForumDaoImpl;
import io.global.forum.ot.ForumMetadata;
import io.global.kv.api.KvClient;
import io.global.ot.api.CommitId;
import io.global.ot.client.OTDriver;
import io.global.ot.service.UserContainer;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import io.global.ot.value.ChangeValueOTSystem;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.util.LogUtils.toLogger;
import static io.global.forum.util.Utils.REGISTRY;

public final class ForumUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(ForumUserContainer.class);

	private final Eventloop eventloop;
	private final KeyPair keys;

	private final OTStateManager<CommitId, ChangeValue<ForumMetadata>> metadataStateManager;

	private final CommGlobalState comm;
	private final ForumDao forumDao;

	private ForumUserContainer(Eventloop eventloop, OTDriver otDriver, KvClient<String, UserId> kvClient, FsClient fsClient, KeyPair keys, CommRepoNames names) {
		this.eventloop = eventloop;
		this.keys = keys;

		this.comm = CommGlobalState.create(eventloop, keys.getPrivKey(), otDriver, kvClient, fsClient, names);
		this.metadataStateManager = comm.createStateManager(names.getMetadata(), REGISTRY.get(new TypeT<ChangeValue<ForumMetadata>>() {}), ChangeValueOTSystem.get(), ChangeValueContainer.of(ForumMetadata.EMPTY));
		this.forumDao = new ForumDaoImpl(this);
	}

	public static ForumUserContainer create(Eventloop eventloop, PrivKey privKey, OTDriver otDriver, KvClient<String, UserId> kvClient, FsClient fsClient, CommRepoNames names) {
		return new ForumUserContainer(eventloop, otDriver, kvClient, fsClient, privKey.computeKeys(), names);
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

	public OTStateManager<CommitId, ChangeValue<ForumMetadata>> getMetadataStateManager() {
		return metadataStateManager;
	}

	public ForumDao getForumDao() {
		return forumDao;
	}

	public CommGlobalState getComm() {
		return comm;
	}

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return comm.getSessionStore();
	}
}
