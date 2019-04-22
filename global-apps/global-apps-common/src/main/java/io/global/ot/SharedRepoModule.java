package io.global.ot;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.OTSystem;
import io.global.ot.client.OTDriver;

public class SharedRepoModule<D> extends AbstractModule {
	private final String repoNamePrefix;

	public SharedRepoModule(String repoNamePrefix) {
		this.repoNamePrefix = repoNamePrefix;
	}

	@Provides
	@Singleton
	DynamicOTNodeServlet<D> provideServlet(OTDriver driver, OTSystem<D> system, StructuredCodec<D> diffCodec) {
		return DynamicOTNodeServlet.create(driver, system, diffCodec, repoNamePrefix);
	}
}
