package io.global.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.ot.OTSystem;
import io.global.ot.client.OTDriver;

public abstract class SharedRepoModule<D> extends AbstractModule {
	private final String repoNamePrefix;

	public SharedRepoModule(String repoNamePrefix) {
		this.repoNamePrefix = repoNamePrefix;
	}

	@Provides
	DynamicOTUplinkServlet<D> provideServlet(OTDriver driver, OTSystem<D> system, StructuredCodec<D> diffCodec) {
		return DynamicOTUplinkServlet.create(driver, system, diffCodec, repoNamePrefix);
	}
}
