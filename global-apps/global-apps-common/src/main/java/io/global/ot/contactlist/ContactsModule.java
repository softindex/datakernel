package io.global.ot.contactlist;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.ot.contactlist.ContactsOTSystem.createOTSystem;
import static io.global.ot.contactlist.ContactsOperation.CONTACTS_OPERATION_CODEC;

public final class ContactsModule extends AbstractModule {
	public static final String REPOSITORY_NAME = "contacts";

	@Provides
	@Singleton
	DynamicOTNodeServlet<ContactsOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), CONTACTS_OPERATION_CODEC, REPOSITORY_NAME);
	}

}
