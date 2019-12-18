package io.global.session;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.global.common.KeyPair;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.KvClient;
import io.global.ot.TypedRepoNames;
import io.global.ot.service.ContainerScope;
import io.global.ot.session.UserId;

public final class KvSessionModule extends AbstractModule {
	@Provides
	@ContainerScope
	KvSessionStore<UserId> kvSessionStore(Eventloop eventloop, KvClient<String, UserId> client, TypedRepoNames names) {
		return KvSessionStore.create(eventloop, client, names.getRepoName(new Key<KvClient<String, UserId>>() {}));
	}

	@Provides
	@ContainerScope
	KvClient<String, UserId> kvClient(GlobalKvDriver<String, UserId> kvDriver, KeyPair keys) {
		return kvDriver.adapt(keys);
	}
}
