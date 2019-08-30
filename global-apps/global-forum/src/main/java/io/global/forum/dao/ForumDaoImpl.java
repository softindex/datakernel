package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.time.CurrentTimeProvider;
import io.global.forum.Utils;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.ot.MapOTStateListenerProxy;
import io.global.forum.pojo.*;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public final class ForumDaoImpl implements ForumDao {
	private final OTStateManager<CommitId, ChangeValue<ForumMetadata>> metadataStateManager;
	private final OTStateManager<CommitId, MapOperation<UserId, UserData>> usersStateManager;
	private final OTStateManager<CommitId, MapOperation<Long, IpBanState>> bansStateManager;
	private final OTStateManager<CommitId, MapOperation<Long, ThreadMetadata>> threadsStateManager;
	private final SessionStore<UserId> sessionStore;

	private final ForumUserContainer container;

	private final ChangeValueContainer<ForumMetadata> metadataView;
	private final Map<UserId, UserData> usersView;
	private final Map<Long, IpBanState> ipBanView;
	private final Map<Long, ThreadMetadata> threadsView;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public ForumDaoImpl(ForumUserContainer container) {
		this.metadataStateManager = container.getMetadataStateManager();
		this.usersStateManager = container.getUsersStateManager();
		this.bansStateManager = container.getBansStateManager();
		this.threadsStateManager = container.getThreadsStateManager();
		this.sessionStore = container.getSessionStore();

		this.container = container;

		metadataView = (ChangeValueContainer<ForumMetadata>) metadataStateManager.getState();
		usersView = ((MapOTState<UserId, UserData>) usersStateManager.getState()).getMap();
		ipBanView = ((MapOTState<Long, IpBanState>) bansStateManager.getState()).getMap();
		threadsView = ((MapOTStateListenerProxy<Long, ThreadMetadata>) threadsStateManager.getState()).getMap();
	}

	@Override
	public Promise<ForumMetadata> getForumMetadata() {
		return Promise.of(metadataView.getValue());
	}

	@Override
	public Promise<Void> setForumMetadata(ForumMetadata metadata) {
		return applyAndSync(metadataStateManager, ChangeValue.of(metadataView.getValue(), metadata, now.currentTimeMillis()));
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
	public Promise<Long> banIpRange(IpRange range, UserId banner, Instant until, String reason) {
		IpBanState state = new IpBanState(new BanState(banner, until, reason), range);
		long id = Utils.generateId();
		return applyAndSync(bansStateManager, MapOperation.forKey(id, SetValue.set(null, state)))
				.map($ -> id);
	}

	@Override
	public Promise<Map<Long, IpBanState>> getBannedRanges() {
		return Promise.of(ipBanView);
	}

	@Override
	public Promise<Boolean> isBanned(InetAddress address) {
		return Promise.of(ipBanView.values().stream().anyMatch(ban -> ban.getIpRange().test(address)));
	}

	@Override
	public Promise<Void> unbanIpRange(long id) {
		return applyAndSync(bansStateManager, MapOperation.forKey(id, SetValue.set(ipBanView.get(id), null)));
	}

	@Override
	public Promise<Map<Long, ThreadMetadata>> getThreads() {
		return Promise.of(threadsView);
	}

	// invariant: just after creating the thread you should do `getThreadDao(id).addRootPost(...)`
	@Override
	public Promise<Long> createThread(ThreadMetadata threadMetadata) {
		long id = Utils.generateId();
		return applyAndSync(threadsStateManager, MapOperation.forKey(id, SetValue.set(null, threadMetadata)))
				.map($ -> id);
	}

	@Override
	public Promise<Void> removeThread(long id) {
		return applyAndSync(threadsStateManager, MapOperation.forKey(id, SetValue.set(threadsView.get(id), null)));
		// ^ this will also remove the state manager because of the listener
	}

	@Override
	@Nullable
	public ThreadDao getThreadDao(long id) {
		return container.getThreadDao(id);
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}

	private static <T> Promise<Void> applyAndSync(OTStateManager<CommitId, T> stateManager, T op) {
		stateManager.add(op);
		return stateManager.sync();
	}
}
