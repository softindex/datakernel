package io.global.ot;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.ot.client.OTDriver;
import io.global.ot.dictionary.DictionaryOperation;

import static io.global.ot.OTUtils.DICTIONARY_OPERATION_CODEC;
import static io.global.ot.dictionary.DictionaryOTSystem.createOTSystem;

public class DictionaryModule extends AbstractModule {
	public final String dictionaryRepoName;

	public DictionaryModule(String dictionaryRepoName) {
		this.dictionaryRepoName = dictionaryRepoName;
	}

	@Provides
	DynamicOTNodeServlet<DictionaryOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), DICTIONARY_OPERATION_CODEC, dictionaryRepoName);
	}
}
