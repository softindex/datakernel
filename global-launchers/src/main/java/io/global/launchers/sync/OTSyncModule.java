package io.global.launchers.sync;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.global.ot.server.GlobalOTNodeImpl;

import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public final class OTSyncModule extends AbstractModule {
	private OTSyncModule() {
	}

	public static OTSyncModule create() {
		return new OTSyncModule();
	}

	@Provides
	@Eager
	@Named("OT update")
	EventloopTaskScheduler updateScheduler(Eventloop eventloop, GlobalOTNodeImpl node, Config config){
		return EventloopTaskScheduler.create(eventloop, node::update)
				.initialize(ofEventloopTaskScheduler(config.getChild("ot.update")));
	}
}
