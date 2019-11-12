package io.global.blog.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.global.blog.dao.BlogDao;
import io.global.blog.ot.BlogMetadata;
import io.global.comm.container.CommState;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.service.UserContainer;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.async.util.LogUtils.toLogger;

public final class BlogUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(BlogUserContainer.class);

	@Inject
	private Eventloop eventloop;

	@Inject
	private BlogDao blogDao;
	@Inject
	private OTStateManager<CommitId, ChangeValue<BlogMetadata>> metadataStateManager;
	@Inject
	private CommState comm;
	@Inject
	private KeyPair keys;

	private BlogUserContainer() {
	}

	@Inject
	public static BlogUserContainer create() {
		return new BlogUserContainer();
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

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return comm.getSessionStore();
	}
}
