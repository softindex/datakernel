package io.global.comm.container;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.remotefs.FsClient;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.ot.session.KvSessionStore;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserId;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static io.datakernel.util.LogUtils.toLogger;

@Inject
public final class CommState implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(CommState.class);

	@Inject
	private Eventloop eventloop;

	@Inject
	private FsClient fsClient;
	@Inject
	private KvSessionStore<UserId> sessionStore;

	@Inject
	private OTStateManager<CommitId, MapOperation<UserId, UserData>> usersStateManager;
	@Inject
	private OTStateManager<CommitId, MapOperation<UserId, InetAddress>> userLastIpManager;
	@Inject
	private OTStateManager<CommitId, MapOperation<String, IpBanState>> bansStateManager;

	@Inject
	private CategoryState root;

	@Inject
	private InstanceProvider<CommDao> commDao;

	public CommDao getCommDao() {
		return commDao.get();
	}

	@NotNull
	public Promise<?> start() {
		return Promises.all(usersStateManager.start(), bansStateManager.start(), userLastIpManager.start(), sessionStore.start(), root.start())
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	public Promise<?> stop() {
		return Promises.all(usersStateManager.stop(), bansStateManager.stop(), userLastIpManager.stop(), sessionStore.stop(), root.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	public FsClient getFsClient() {
		return fsClient;
	}

	public Promise<@Nullable ThreadDao> getThreadDao(String threadId) {
		Promise<ThreadDao> promise = root.getThreadDaos().get(threadId);
		return promise != null ? promise : Promise.of(null);
	}

	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}
}
