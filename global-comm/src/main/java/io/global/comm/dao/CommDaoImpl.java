package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.global.comm.container.CommGlobalState;
import io.global.comm.ot.MapOTStateListenerProxy;
import io.global.comm.pojo.*;
import io.global.comm.util.Utils;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public final class CommDaoImpl implements CommDao {
	private final OTStateManager<CommitId, MapOperation<UserId, UserData>> usersStateManager;
	private final OTStateManager<CommitId, MapOperation<String, IpBanState>> bansStateManager;
	private final OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> threadsStateManager;

	private final CommGlobalState container;

	private final Map<UserId, UserData> usersView;
	private final Map<String, IpBanState> ipBanView;
	private final Map<String, ThreadMetadata> threadsView;

	public CommDaoImpl(CommGlobalState container) {
		this.usersStateManager = container.getUsersStateManager();
		this.bansStateManager = container.getBansStateManager();
		this.threadsStateManager = container.getThreadsStateManager();

		this.container = container;

		usersView = ((MapOTState<UserId, UserData>) usersStateManager.getState()).getMap();
		ipBanView = ((MapOTState<String, IpBanState>) bansStateManager.getState()).getMap();
		threadsView = ((MapOTStateListenerProxy<String, ThreadMetadata>) threadsStateManager.getState()).getMap();
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
	public Promise<@Nullable UserData> getUser(UserId userId) {
		return Promise.of(usersView.get(userId));
	}

	@Override
	public Promise<Void> updateUser(UserId userId, UserData userData) {
		return applyAndSync(usersStateManager, MapOperation.forKey(userId, SetValue.set(usersView.get(userId), userData)));
	}

	@Override
	public Promise<Set<UserId>> listKnownUsers() {
		return Promise.of(usersView.keySet());
	}

	@Override
	public Promise<String> banIpRange(IpRange range, UserId banner, Instant until, String reason) {
		IpBanState state = new IpBanState(new BanState(banner, until, reason), range);
		String id = Utils.generateId();
		return applyAndSync(bansStateManager, MapOperation.forKey(id, SetValue.set(null, state)))
				.map($ -> id);
	}

	@Override
	public Promise<Map<String, IpBanState>> getBannedRanges() {
		return Promise.of(ipBanView);
	}

	@Override
	public Promise<Boolean> isBanned(InetAddress address) {
		return Promise.of(ipBanView.values().stream().anyMatch(ban -> ban.getIpRange().test(address)));
	}

	@Override
	public Promise<Void> unbanIpRange(String id) {
		return applyAndSync(bansStateManager, MapOperation.forKey(id, SetValue.set(ipBanView.get(id), null)));
	}

	@Override
	public Promise<Map<String, ThreadMetadata>> getThreads() {
		return Promise.of(threadsView);
	}

	@Override
	public Promise<String> generateThreadId() {
		String postId;
		do {
			postId = Utils.generateId();
		} while (threadsView.containsKey(postId));
		return Promise.of(postId);
	}

	@Override
	public Promise<Void> updateThread(String threadId, ThreadMetadata threadMetadata) {
		return applyAndSync(threadsStateManager, MapOperation.forKey(threadId, SetValue.set(threadsView.get(threadId), threadMetadata)));
	}

	@Override
	public Promise<Void> updateThreadTitle(String threadId, String title) {
		return applyAndSync(threadsStateManager, MapOperation.forKey(threadId, SetValue.set(threadsView.get(threadId), new ThreadMetadata(title))));
	}

	@Override
	public Promise<Void> removeThread(String id) {
		return applyAndSync(threadsStateManager, MapOperation.forKey(id, SetValue.set(threadsView.get(id), null)));
		// ^ this will also remove the state manager because of the listener
	}

	private static <T> Promise<Void> applyAndSync(OTStateManager<CommitId, T> stateManager, T op) {
		stateManager.add(op);
		return stateManager.sync();
	}
}
