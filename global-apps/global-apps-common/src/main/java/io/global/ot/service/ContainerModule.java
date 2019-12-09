package io.global.ot.service;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.ofEventloopTaskSchedule;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static io.global.ot.service.ContainerManagerImpl.DEFAULT_SYNC_SCHEDULE;

public abstract class ContainerModule<C extends UserContainer> extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(ContainerModule.class);

	@Override
	protected void configure() {
		bind(PrivKey.class).in(ContainerScope.class).to(() -> {
			throw new AssertionError("No PrivKey was put in a ContainerScope, this should never happen");
		});
		bind(KeyPair.class).in(ContainerScope.class).to(PrivKey::computeKeys, PrivKey.class);
	}

	@Provides
	KeyExchanger keyExchanger(Eventloop eventloop, Executor executor, Path containersPath) {
		return FsKeyExchanger.create(eventloop, executor, containersPath, true);
	}

	@Provides
	ContainerManager<C> containerManager(Injector injector, Eventloop eventloop, KeyExchanger keyExchanger, Config config, Key<C> containerKey) {
		PrivKey privKey = config.get(ofPrivKey(), "privateKey", null);
		if (privKey == null) {
			logger.info("No private key specified, running in multi container mode");
			return ContainerManagerImpl.create(injector, containerKey, eventloop, keyExchanger)
					.withSyncSchedule(config.get(ofEventloopTaskSchedule(), "container.sync", DEFAULT_SYNC_SCHEDULE));
		} else {
			logger.info("Private key specified, running in single container mode");
			return SingleContainerManager.create(injector, containerKey, eventloop, privKey);
		}
	}

	@Provides
	ContainerServlet serviceEnsuringServlet(ContainerManager<C> containerManager, AsyncServlet servlet) {
		return ContainerServlet.create(containerManager, servlet);
	}
}
