package io.global.ot.contactlist;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.ot.contactlist.ContactsOTSystem.createOTSystem;
import static io.global.ot.contactlist.ContactsOperation.CONTACTS_OPERATION_CODEC;

public final class ContactsModule extends AbstractModule {
	public static final String REPOSITORY_NAME = "contacts";

	@Provides
	DynamicOTNodeServlet<ContactsOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), CONTACTS_OPERATION_CODEC, REPOSITORY_NAME);
	}

}
