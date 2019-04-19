package io.global.editor.documentlist;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.global.editor.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.editor.Utils.DOCUMENT_LIST_OPERATION_CODEC;
import static io.global.editor.documentlist.DocumentListOTSystem.createOTSystem;

public final class DocumentListModule extends AbstractModule {
	public static final String REPOSITORY_NAME = "editor/index";

	@Provides
	@Singleton
	DynamicOTNodeServlet<DocumentListOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), DOCUMENT_LIST_OPERATION_CODEC, REPOSITORY_NAME);
	}
}
