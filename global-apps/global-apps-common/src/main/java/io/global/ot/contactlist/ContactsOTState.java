package io.global.ot.contactlist;

import io.datakernel.ot.OTState;
import io.global.common.PubKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ContactsOTState implements OTState<ContactsOperation> {
	private final Map<PubKey, String> contacts = new HashMap<>();

	@Override
	public void init() {
		contacts.clear();
	}

	@Override
	public void apply(ContactsOperation op) {
		op.apply(contacts);
	}

	public Map<PubKey, String> getContacts() {
		return Collections.unmodifiableMap(contacts);
	}
}
