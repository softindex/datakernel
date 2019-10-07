package io.global.ot.service;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.global.common.PrivKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public abstract class ContainerModule<T extends UserContainer> extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(ContainerModule.class);

	@Provides
	KeyExchanger keyExchanger(Eventloop eventloop, Executor executor, Path containersPath) {
		return FsKeyExchanger.create(eventloop, executor, containersPath, true);
	}

	@Provides
	ContainerManager<T> containerManager(Eventloop eventloop, KeyExchanger keyExchanger, BiFunction<Eventloop, PrivKey, T> containerFactory, Config config) throws ParseException {
		String privateKey = config.get("privateKey", null);
		if (privateKey == null) {
			logger.info("No private key specified, running in multi container mode");
			return ContainerManagerImpl.create(eventloop, keyExchanger, containerFactory);
		}
		PrivKey privKey = PrivKey.fromString(privateKey);
		T singleContainer = containerFactory.apply(eventloop, privKey);
		logger.info("Private key specified, running in single container mode");
		return SingleContainerManager.of(singleContainer);
	}

	@Provides
	ContainerServlet serviceEnsuringServlet(ContainerManager<T> containerManager, AsyncServlet servlet) {
		return ContainerServlet.create(containerManager, servlet);
	}
}
