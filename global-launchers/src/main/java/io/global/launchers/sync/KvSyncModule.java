package io.global.launchers.sync;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.global.kv.GlobalKvNodeImpl;

import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public final class KvSyncModule extends AbstractModule {
	@Provides
	@Eager
	@Named("KV push")
	EventloopTaskScheduler pushScheduler(Eventloop eventloop, GlobalKvNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("kv.push")));
	}

	@Provides
	@Eager
	@Named("KV catch up")
	EventloopTaskScheduler catchUpScheduler(Eventloop eventloop, GlobalKvNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("kv.catchUp")));
	}
}
