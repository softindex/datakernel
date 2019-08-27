package io.global.forum.dao;

import io.datakernel.async.Promise;
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

	Promise<Long> banIpRange(IpRange range, UserId banner, Instant until, String description);

	Promise<Map<Long, IpBanState>> getBannedRanges();

	Promise<Boolean> isBanned(InetAddress address);

	Promise<Void> unbanIpRange(long id);

	Promise<Long> createThread(ThreadMetadata threadMetadata, UserId author);

	Promise<Map<Long, ThreadMetadata>> getThreads();

	Promise<Void> removeThread(long id);

	Promise<@Nullable ThreadDao> getThreadDao(long id);
}
