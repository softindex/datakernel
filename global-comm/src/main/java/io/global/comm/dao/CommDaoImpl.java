package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Inject;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.global.comm.container.CommState;
import io.global.comm.ot.MapOTStateListenerProxy;
import io.global.comm.ot.session.KvSessionStore;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserId;
import io.global.comm.util.OTPagedAsyncMap;
import io.global.comm.util.PagedAsyncMap;
import io.global.comm.util.Utils;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOperation;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.Map;

import static java.util.Comparator.comparingLong;
import static java.util.Map.Entry.comparingByValue;

public final class CommDaoImpl implements CommDao {
	private final CommState container;

	private final PagedAsyncMap<String, ThreadMetadata> threads;
	private final Map<String, ThreadMetadata> threadsView;

	@Inject
	private KeyPair keys;

	@Inject
	private PagedAsyncMap<UserId, UserData> users;
	@Inject
	private PagedAsyncMap<UserId, InetAddress> lastIps;
	@Inject
	private PagedAsyncMap<String, IpBanState> ipBans;

	@Inject
	private KvSessionStore<UserId> sessionStore;

	@Inject
	public CommDaoImpl(CommState container, OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> threadStateManager) {
		this.container = container;

		threadsView = ((MapOTStateListenerProxy<String, ThreadMetadata>) threadStateManager.getState()).getMap();
		threads = new OTPagedAsyncMap<>(threadStateManager, comparingByValue(comparingLong(ThreadMetadata::getLastUpdate).reversed()));
	}

	@Override
	public Promise<@Nullable ThreadDao> getThreadDao(String id) {
		return container.getThreadDao(id);
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}

	@Override
	public PagedAsyncMap<UserId, UserData> getUsers() {
		return users;
	}

	@Override
	public PagedAsyncMap<UserId, InetAddress> getUserLastIps() {
		return lastIps;
	}

	@Override
	public PagedAsyncMap<String, IpBanState> getIpBans() {
		return ipBans;
	}

	@Override
	public PagedAsyncMap<String, ThreadMetadata> getThreads(String category) {
		return threads;
	}

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	@Override
	public Promise<String> generateThreadId() {
		String postId;
		do { // if this ever runs two times then it was worth it
			postId = Utils.generateId();
		} while (threadsView.containsKey(postId));
		return Promise.of(postId);
	}
}
