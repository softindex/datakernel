package io.global.comm.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserId;
import io.global.comm.util.PagedAsyncMap;
import io.global.common.KeyPair;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;

public interface CommDao {
	KeyPair getKeys();

	SessionStore<UserId> getSessionStore();

	@Nullable
	ThreadDao getThreadDao(String id);

	PagedAsyncMap<UserId, UserData> getUsers();

	PagedAsyncMap<UserId, InetAddress> getUserLastIps();

	PagedAsyncMap<String, IpBanState> getIpBans();

	PagedAsyncMap<String, ThreadMetadata> getThreads();

	Promise<String> generateThreadId();
}
