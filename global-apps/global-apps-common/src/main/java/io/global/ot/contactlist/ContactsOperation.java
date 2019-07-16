package io.global.ot.contactlist;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.common.PubKey;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.object;
import static io.global.Utils.PUB_KEY_HEX_CODEC;

public final class ContactsOperation {
	public static final ContactsOperation EMPTY = new ContactsOperation(null, "", true);
	public static final StructuredCodec<ContactsOperation> CONTACTS_OPERATION_CODEC = object(ContactsOperation::new,
			"pubKey", ContactsOperation::getPubKey, PUB_KEY_HEX_CODEC.nullable(),
			"name", ContactsOperation::getName, STRING_CODEC,
			"remove", ContactsOperation::isRemove, StructuredCodecs.BOOLEAN_CODEC);

	private final PubKey pubKey;
	private final String name;
	private final boolean remove;

	private ContactsOperation(PubKey pubKey, String name, boolean remove) {
		this.pubKey = pubKey;
		this.name = name;
		this.remove = remove;
	}

	public static ContactsOperation add(PubKey pubKey, String name) {
		return new ContactsOperation(pubKey, name, false);
	}

	public static ContactsOperation remove(PubKey pubKey, String name) {
		return new ContactsOperation(pubKey, name, true);
	}

	public void apply(Map<PubKey, String> contacts) {
		if (isEmpty()) {
			return;
		}

		if (remove) {
			contacts.remove(pubKey);
		} else {
			contacts.put(pubKey, name);
		}
	}

	public PubKey getPubKey() {
		assert pubKey != null;
		return pubKey;
	}

	public String getName() {
		return name;
	}

	public boolean isRemove() {
		return remove;
	}

	public ContactsOperation invert() {
		return new ContactsOperation(pubKey, name, !remove);
	}

	public boolean isEmpty() {
		return pubKey == null;
	}

	public boolean isInversionFor(ContactsOperation other) {
		return pubKey != null && other.pubKey != null &&
				pubKey.equals(other.pubKey) &&
				name.equals(other.name) &&
				remove != other.remove;
	}
}
