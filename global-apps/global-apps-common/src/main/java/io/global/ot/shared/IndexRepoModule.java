package io.global.ot.shared;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
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
	@Singleton
	DynamicOTNodeServlet<SharedReposOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), SHARED_REPO_OPERATION_CODEC, indexRepo);
	}
}
