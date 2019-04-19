package io.global.editor.document;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.global.editor.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.editor.Utils.DOCUMENT_MULTI_OPERATION_CODEC;
import static io.global.editor.Utils.createMergedOTSystem;

public final class DocumentModule extends AbstractModule {
	public static final String DOCUMENT_PREFIX = "editor/document";

	@Provides
	@Singleton
	DynamicOTNodeServlet<DocumentMultiOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createMergedOTSystem(), DOCUMENT_MULTI_OPERATION_CODEC, DOCUMENT_PREFIX);
	}
}
