package io.global.ot.shared;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.client.OTDriver;

import static io.global.ot.OTUtils.SHARED_REPOS_OPERATION_CODEC;
import static io.global.ot.shared.SharedReposOTSystem.createOTSystem;

public final class IndexRepoModule extends AbstractModule {
	private final String indexRepo;

	public IndexRepoModule(String indexRepo) {
		this.indexRepo = indexRepo;
	}

	@Provides
	DynamicOTUplinkServlet<SharedReposOperation> provideServlet(OTDriver driver) {
		return DynamicOTUplinkServlet.create(driver, createOTSystem(), SHARED_REPOS_OPERATION_CODEC, indexRepo);
	}
}
