package io.global.ot.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.ot.OTSystem;
import io.global.ot.client.OTDriver;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.service.messaging.MessagingServlet;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.GlobalPmNode;

import static io.global.ot.OTUtils.SHARED_REPO_MESSAGE_CODEC;

public class UserContainerModule<D> extends AbstractModule {
	private final String indexRepo;
	private final String repoPrefix;

	public UserContainerModule(String indexRepo, String repoPrefix) {
		this.indexRepo = indexRepo;
		this.repoPrefix = repoPrefix;
	}

	@Provides
	@Singleton
	UserContainerHolder<D> provideUserContainerHolder(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem,
			GlobalPmDriver<CreateSharedRepo> pmDriver, StructuredCodec<D> diffCodec) {
		return UserContainerHolder.create(eventloop, driver, otSystem, diffCodec, pmDriver, indexRepo, repoPrefix);
	}

	@Provides
	@Singleton
	ServiceEnsuringServlet providePrivKeyEnsuringServlet(UserContainerHolder<D> userContainerHolder, MiddlewareServlet servlet) {
		return ServiceEnsuringServlet.create(userContainerHolder, servlet);
	}

	@Provides
	@Singleton
	MessagingServlet provideMessagingServlet(UserContainerHolder<D> userContainerHolder) {
		return MessagingServlet.create(userContainerHolder);
	}

	@Provides
	@Singleton
	GlobalPmDriver<CreateSharedRepo> providePmDriver(GlobalPmNode pmNode) {
		return new GlobalPmDriver<>(pmNode, SHARED_REPO_MESSAGE_CODEC);
	}
}
