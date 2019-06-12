package io.global.ot.shared;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.ot.OTUtils.SHARED_REPO_OPERATION_CODEC;
import static io.global.ot.shared.SharedReposOTSystem.createOTSystem;

public final class IndexRepoModule extends AbstractModule {
	private final String indexRepo;

	public IndexRepoModule(String indexRepo) {
		this.indexRepo = indexRepo;
	}

	@Provides
	DynamicOTNodeServlet<SharedReposOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), SHARED_REPO_OPERATION_CODEC, indexRepo);
	}
}
