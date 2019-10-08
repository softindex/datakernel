package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.global.comm.container.CommGlobalState;
import io.global.comm.ot.MapOTStateListenerProxy;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserId;
import io.global.comm.util.OTPagedAsyncMap;
import io.global.comm.util.PagedAsyncMap;
import io.global.comm.util.Utils;
import io.global.common.KeyPair;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.Map;

import static java.util.Comparator.comparingLong;
import static java.util.Map.Entry.comparingByValue;

public final class CommDaoImpl implements CommDao {
	private final CommGlobalState container;

	private final Map<String, ThreadMetadata> threadsView;

	private final PagedAsyncMap<UserId, UserData> users;
	private final PagedAsyncMap<UserId, InetAddress> lastIps;
	private final PagedAsyncMap<String, IpBanState> ipBans;
	private final PagedAsyncMap<String, ThreadMetadata> threads;

	public CommDaoImpl(CommGlobalState container) {
		this.container = container;

		threadsView = ((MapOTStateListenerProxy<String, ThreadMetadata>) container.getThreadsStateManager().getState()).getMap();

		users = new OTPagedAsyncMap<>(container.getUsersStateManager());
		lastIps = new OTPagedAsyncMap<>(container.getLastIpsStateManager());
		ipBans = new OTPagedAsyncMap<>(container.getBansStateManager());

		threads = new OTPagedAsyncMap<>(
				container.getThreadsStateManager(),
				((MapOTStateListenerProxy<String, ThreadMetadata>) container.getThreadsStateManager().getState()).getMap(),
				comparingByValue(comparingLong(ThreadMetadata::getLastUpdate).reversed())
		);
	}

	@Override
	public KeyPair getKeys() {
		return container.getKeys();
	}

	@Override
	@Nullable
	public ThreadDao getThreadDao(String id) {
		return container.getThreadDao(id);
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return container.getSessionStore();
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
	public PagedAsyncMap<String, ThreadMetadata> getThreads() {
		return threads;
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
