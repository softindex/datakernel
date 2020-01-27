package io.global.comm.dao;

import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.di.annotation.Inject;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.global.comm.container.CommState;
import io.global.comm.ot.AppMetadata;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.util.OTPagedAsyncMap;
import io.global.comm.util.PagedAsyncMap;
import io.global.comm.util.Utils;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTStateListenerProxy;
import io.global.ot.map.MapOperation;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.Map;

import static java.util.Comparator.comparingLong;
import static java.util.Map.Entry.comparingByValue;

public final class CommDaoImpl implements CommDao {
	private final CommState container;

	private final PagedAsyncMap<String, ThreadMetadata> threads;
	private final Map<String, ThreadMetadata> threadsView;

	private final OTStateManager<CommitId, ChangeValue<AppMetadata>> metadataStateManager;
	private final ChangeValueContainer<AppMetadata> metadataView;

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

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	@Inject
	public CommDaoImpl(CommState container, OTStateManager<CommitId, ChangeValue<AppMetadata>> metadataStateManager, OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> threadStateManager) {
		this.container = container;
		this.metadataStateManager = metadataStateManager;

		metadataView = (ChangeValueContainer<AppMetadata>) metadataStateManager.getState();

		threads = new OTPagedAsyncMap<>(threadStateManager, comparingByValue(comparingLong(ThreadMetadata::getLastUpdate).reversed()));
		threadsView = ((MapOTStateListenerProxy<String, ThreadMetadata>) threadStateManager.getState()).getMap();
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

	@Override
	public Promise<AppMetadata> getAppMetadata() {
		return Promise.of(metadataView.getValue());
	}

	@Override
	public Promise<Void> setAppMetadata(AppMetadata metadata) {
		return applyAndSync(metadataStateManager, ChangeValue.of(metadataView.getValue(), metadata, now.currentTimeMillis()));
	}

	private static <T> Promise<Void> applyAndSync(OTStateManager<CommitId, T> stateManager, T op) {
		stateManager.add(op);
		return stateManager.sync();
	}
}
