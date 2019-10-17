package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.IpRange;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.common.KeyPair;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public interface CommDao {
	KeyPair getKeys();

	SessionStore<UserId> getSessionStore();

	@Nullable
	ThreadDao getThreadDao(String id);

	Promise<@Nullable UserData> getUser(UserId userId);

	Promise<Void> updateUser(UserId userId, UserData userData);

	Promise<Set<UserId>> listKnownUsers();

	Promise<String> banIpRange(IpRange range, UserId banner, Instant until, String description);

	Promise<Map<String, IpBanState>> getBannedRanges();

	Promise<Boolean> isBanned(InetAddress address);

	Promise<Void> unbanIpRange(String id);

	Promise<String> generateThreadId();

	Promise<Void> updateThread(String threadId, ThreadMetadata threadMetadata);

	Promise<Void> updateThreadTitle(String threadId, String title);

	Promise<Map<String, ThreadMetadata>> getThreads();

	Promise<Void> removeThread(String id);
}
