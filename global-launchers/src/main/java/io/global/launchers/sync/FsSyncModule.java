package io.global.launchers.sync;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.global.fs.local.GlobalFsNodeImpl;

import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public final class FsSyncModule extends AbstractModule {
	@Provides
	@Eager
	@Named("FS push")
	EventloopTaskScheduler pushScheduler(Eventloop eventloop, GlobalFsNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("fs.push")));
	}

	@Provides
	@Eager
	@Named("FS catch up")
	EventloopTaskScheduler catchUpScheduler(Eventloop eventloop, GlobalFsNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("fs.catchUp")));
	}
}
