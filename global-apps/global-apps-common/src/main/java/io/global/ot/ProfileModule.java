package io.global.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.ot.client.OTDriver;
import io.global.ot.map.MapOperation;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.global.ot.OTUtils.getMapOperationCodec;
import static io.global.ot.map.MapOTSystem.createOTSystem;

public class ProfileModule extends AbstractModule {
	public static final String PROFILE_REPO_NAME = "profile";

	@Provides
	DynamicOTNodeServlet<MapOperation<String, String>> provideServlet(OTDriver driver) {
		StructuredCodec<MapOperation<String, String>> mapOperationCodec = getMapOperationCodec(STRING_CODEC, STRING_CODEC);
		return DynamicOTNodeServlet.create(driver, createOTSystem(String::compareTo), mapOperationCodec, PROFILE_REPO_NAME);
	}
}
