package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.util.guice.SimpleModule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.datakernel.config.ConfigConverters.constrain;
import static io.datakernel.config.ConfigConverters.ofInteger;

/**
 * This module provides an unnamed singleton {@link ExecutorService executor} instance.
 */
public class ExecutorServiceModule extends SimpleModule {

	// region creators
	private ExecutorServiceModule(){
	}

	public static ExecutorServiceModule create() {
		return new ExecutorServiceModule();
	}
	// endregion

	@Provides
	@Singleton
	public ExecutorService provide(Config config) {
		Integer keepAlive = config.get(constrain(ofInteger(), x -> x >= 0), "threadpool.keepAliveSeconds", 60);
		Integer corePoolSize = config.get(constrain(ofInteger(), x -> x >= 0), "threadpool.corePoolSize", 0);
		Integer maxPoolSize = config.get(constrain(ofInteger(), x -> x <= 0 || x < corePoolSize ), "threadpool.maxPoolSize", 0);
		return new ThreadPoolExecutor(
				corePoolSize,
				maxPoolSize <= 0 ? Integer.MAX_VALUE : maxPoolSize,
				keepAlive,
				TimeUnit.SECONDS,
				new SynchronousQueue<>());
	}
}
