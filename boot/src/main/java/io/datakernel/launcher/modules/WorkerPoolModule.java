package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.WorkerPool;

import static io.datakernel.config.ConfigConverters.ofInteger;

public class WorkerPoolModule extends SimpleModule {
	public static final int DEFAULT_NUMBER_OF_WORKERS = 4;

	// region creators
	private WorkerPoolModule() {
	}

	public static WorkerPoolModule create() {
		return new WorkerPoolModule();
	}
	// endregion

	@Provides
	@Singleton
	public WorkerPool provide(Config config) {
		return new WorkerPool(config.get(ofInteger(), "workers", DEFAULT_NUMBER_OF_WORKERS));
	}
}
