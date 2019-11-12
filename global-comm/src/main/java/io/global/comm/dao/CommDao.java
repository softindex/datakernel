package io.global.comm.dao;

import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.util.PagedAsyncMap;
import io.global.common.KeyPair;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;

public interface CommDao {
	KeyPair getKeys();

	SessionStore<UserId> getSessionStore();

	Promise<@Nullable ThreadDao> getThreadDao(String id);

	PagedAsyncMap<UserId, UserData> getUsers();

	PagedAsyncMap<UserId, InetAddress> getUserLastIps();

	PagedAsyncMap<String, IpBanState> getIpBans();

	PagedAsyncMap<String, ThreadMetadata> getThreads(String category);

	Promise<String> generateThreadId();
}
