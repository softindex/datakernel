package io.global.appstore;

import io.datakernel.async.Promise;
import io.global.appstore.pojo.AppInfo;
import io.global.appstore.pojo.HostingInfo;
import io.global.appstore.pojo.Profile;
import io.global.appstore.pojo.User;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public interface AppStoreApi {
	Promise<Profile> exchangeAuthToken(String authToken);

	Promise<@Nullable User> findUserByPublicKey(PubKey pubKey);

	Promise<Map<PubKey, User>> lookUp(String lookUpString, @Nullable Integer limit, @Nullable Integer offset);

	Promise<Set<AppInfo>> listApps();

	Promise<Set<HostingInfo>> listHostings();
}
