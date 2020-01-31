package io.datakernel.vlog.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.vlog.dao.AppDao;
import io.datakernel.vlog.handler.ProgressListener;
import io.datakernel.vlog.handler.VideoMultipartHandler;
import io.datakernel.vlog.ot.VlogMetadata;
import io.global.api.AppDir;
import io.global.comm.container.CommState;
import io.global.common.KeyPair;
import io.global.fs.local.GlobalFsNodeImpl;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.UserContainer;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.datakernel.async.util.LogUtils.toLogger;

public final class VlogUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(VlogUserContainer.class);

	private final OTStateManager<CommitId, ChangeValue<VlogMetadata>> metadataStateManager;
	private final Map<String, ProgressListener> progressListenerMap;
	private final Eventloop eventloop;
	private final CommState comm;
	private final AppDao appDao;
	private VideoMultipartHandler videoMultipartHandler;
	private final KeyPair keys;
	private final GlobalFsNodeImpl fsNode;
	private final GlobalOTNodeImpl otNode;
	private final TypedRepoNames names;
	private final String appDir;

	@Inject
	public VlogUserContainer(OTStateManager<CommitId, ChangeValue<VlogMetadata>> metadataStateManager, Eventloop eventloop,
			KeyPair keys, CommState comm, AppDao appDao, Map<String, ProgressListener> progressListenerMap,
			VideoMultipartHandler videoMultipartHandler, GlobalFsNodeImpl fsNode, GlobalOTNodeImpl otNode,
			TypedRepoNames names, @AppDir String appDir) {
		this.metadataStateManager = metadataStateManager;
		this.progressListenerMap = progressListenerMap;
		this.eventloop = eventloop;
		this.keys = keys;
		this.comm = comm;
		this.appDao = appDao;
		this.videoMultipartHandler = videoMultipartHandler;
		this.fsNode = fsNode;
		this.otNode = otNode;
		this.names = names;
		this.appDir = appDir;
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
				.then($ -> otNode.fetch(RepoID.of(keys, names.getRepoName(new Key<ChangeValue<VlogMetadata>>() {}))))
				.thenEx(($, e) -> metadataStateManager.start())
				.then($ -> fsNode.fetch(keys.getPubKey(), appDir + "/**").toTry().toVoid())
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		return comm.stop()
				.then($ -> metadataStateManager.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	public AppDao getAppDao() {
		return appDao;
	}

	public Map<String, ProgressListener> getProgressListenerMap() {
		return progressListenerMap;
	}

	public VideoMultipartHandler getVideoMultipartHandler() {
		return videoMultipartHandler;
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
