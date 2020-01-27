package io.global.comm.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.api.AppDir;
import io.global.comm.dao.CommDao;
import io.global.comm.ot.AppMetadata;
import io.global.common.KeyPair;
import io.global.fs.local.GlobalFsNodeImpl;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.RepoID;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.UserContainer;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.async.util.LogUtils.toLogger;

@Inject
public final class CommUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(CommUserContainer.class);

	@Inject
	private Eventloop eventloop;

	@Inject
	private StateManagerWithMerger<ChangeValue<AppMetadata>> metadataStateManagerWithMerger;

	@Inject
	private CommState comm;

	@Inject
	private CommDao commDao;

	@Inject
	private KeyPair keys;

	@Inject
	@AppDir
	private String appDir;

	@Inject
	private GlobalFsNodeImpl fsNode;

	@Inject
	private KvSessionStore<UserId> sessionStore;

	@Inject
	private GlobalOTNodeImpl otNode;

	@Inject
	private TypedRepoNames names;

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		return comm.start()
				.then($ -> otNode.fetch(RepoID.of(keys, names.getRepoName(new Key<ChangeValue<AppMetadata>>() {}))))
				.thenEx(($, e) -> metadataStateManagerWithMerger.start())
				.whenResult($ -> fsNode.fetch(keys.getPubKey(), appDir + "/**"))
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		return comm.stop()
				.then($ -> metadataStateManagerWithMerger.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	public CommDao getCommDao() {
		return commDao;
	}

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}
}
