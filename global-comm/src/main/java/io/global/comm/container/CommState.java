package io.global.comm.container;

import io.datakernel.async.service.EventloopService;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.UserData;
import io.global.common.KeyPair;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.RepoID;
import io.global.ot.map.MapOperation;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.session.UserId;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static io.datakernel.async.util.LogUtils.toLogger;

@Inject
public final class CommState implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(CommState.class);

	@Inject
	private Eventloop eventloop;

	@Inject
	private KvSessionStore<UserId> sessionStore;

	@Inject
	private StateManagerWithMerger<MapOperation<UserId, UserData>> usersStateManagerWithMerger;
	@Inject
	private StateManagerWithMerger<MapOperation<UserId, InetAddress>> userLastIpManagerWithMerger;
	@Inject
	private StateManagerWithMerger<MapOperation<String, IpBanState>> bansStateManagerWithMerger;

	@Inject
	private CategoryState root;

	@Inject
	private KeyPair keys;
	@Inject
	private GlobalOTNodeImpl node;
	@Inject
	private TypedRepoNames names;

	@NotNull
	public Promise<?> start() {
		return node.fetch(RepoID.of(keys, names.getRepoName(new Key<MapOperation<UserId, UserData>>() {})))
				.thenEx(($, e) -> node.fetch(RepoID.of(keys, names.getRepoName(new Key<MapOperation<UserId, InetAddress>>() {}))))
				.thenEx(($, e) -> node.fetch(RepoID.of(keys, names.getRepoName(new Key<MapOperation<String, IpBanState>>() {}))))
				.thenEx(($, e) -> Promises.all(
						usersStateManagerWithMerger.start(),
						bansStateManagerWithMerger.start(),
						userLastIpManagerWithMerger.start(),
						sessionStore.start(),
						root.start()))
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	public Promise<?> stop() {
		return Promises.all(usersStateManagerWithMerger.stop(), bansStateManagerWithMerger.stop(), userLastIpManagerWithMerger.stop(), sessionStore.stop(), root.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Promise<@Nullable ThreadDao> getThreadDao(String threadId) {
		Promise<ThreadDao> promise = root.getThreadDaos().get(threadId);
		return promise != null ? promise : Promise.of(null);
	}
}
