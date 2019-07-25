package io.global.ot.service;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.ot.OTSystem;
import io.global.ot.client.OTDriver;
import io.global.ot.service.messaging.CreateSharedRepo;
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
	UserContainerHolder<D> provideUserContainerHolder(Eventloop eventloop, OTDriver driver, OTSystem<D> otSystem,
			GlobalPmDriver<CreateSharedRepo> pmDriver, StructuredCodec<D> diffCodec) {
		return UserContainerHolder.create(eventloop, driver, otSystem, diffCodec, pmDriver, indexRepo, repoPrefix);
	}

	@Provides
	AsyncServlet providePrivKeyEnsuringServlet(UserContainerHolder<D> userContainerHolder, RoutingServlet servlet) {
		return ServiceEnsuringServlet.create(userContainerHolder, servlet);
	}

	@Provides
	GlobalPmDriver<CreateSharedRepo> providePmDriver(GlobalPmNode pmNode) {
		return new GlobalPmDriver<>(pmNode, SHARED_REPO_MESSAGE_CODEC);
	}
}
