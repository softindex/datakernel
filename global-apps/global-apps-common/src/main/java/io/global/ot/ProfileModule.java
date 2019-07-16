package io.global.ot;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.ot.client.OTDriver;
import io.global.ot.dictionary.DictionaryOperation;

import static io.global.ot.OTUtils.DICTIONARY_OPERATION_CODEC;
import static io.global.ot.dictionary.DictionaryOTSystem.createOTSystem;

public class ProfileModule extends AbstractModule {
	public static final String PROFILE_REPO_NAME = "profile";

	@Provides
	DynamicOTNodeServlet<DictionaryOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), DICTIONARY_OPERATION_CODEC, PROFILE_REPO_NAME);
	}
}
