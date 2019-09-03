package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.pojo.*;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public interface ForumDao {
	Promise<ForumMetadata> getForumMetadata();

	Promise<Void> setForumMetadata(ForumMetadata metadata);

	Promise<@Nullable UserData> getUser(UserId userId);

	Promise<Void> updateUser(UserId userId, UserData userData);

	Promise<Set<UserId>> listKnownUsers();

	Promise<String> banIpRange(IpRange range, UserId banner, Instant until, String description);

	Promise<Map<String, IpBanState>> getBannedRanges();

	Promise<Boolean> isBanned(InetAddress address);

	Promise<Void> unbanIpRange(String id);

	Promise<String> createThread(ThreadMetadata threadMetadata);

	Promise<Map<String, ThreadMetadata>> getThreads();

	Promise<Void> removeThread(String id);

	@Nullable
	ThreadDao getThreadDao(String id);

	SessionStore<UserId> getSessionStore();
}
