package io.global.blog.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.api.AppDir;
import io.global.blog.dao.BlogDao;
import io.global.blog.ot.BlogMetadata;
import io.global.comm.container.CommState;
import io.global.common.KeyPair;
import io.global.fs.local.GlobalFsNodeImpl;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.RepoID;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.UserContainer;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.async.util.LogUtils.toLogger;

@Inject
public final class BlogUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(BlogUserContainer.class);

	@Inject
	private Eventloop eventloop;
	@Inject
	private BlogDao blogDao;
	@Inject
	private StateManagerWithMerger<ChangeValue<BlogMetadata>> metadataStateManagerWithMerger;
	@Inject
	private CommState comm;
	@Inject
	private KeyPair keys;

	@Inject
	@AppDir
	private String appDir;

	@Inject
	private GlobalFsNodeImpl fsNode;

	@Inject
	private GlobalOTNodeImpl otNode;
	@Inject
	TypedRepoNames names;

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		return comm.start()
				.then($ -> otNode.fetch(RepoID.of(keys, names.getRepoName(new Key<ChangeValue<BlogMetadata>>() {}))))
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

	public BlogDao getBlogDao() {
		return blogDao;
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
