package io.global.ot.friendlist;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.global.Utils.PUB_KEY_HEX_CODEC;

public final class ContactsOperation {
	public static final ContactsOperation EMPTY = new ContactsOperation(null, true);
	public static final StructuredCodec<ContactsOperation> CONTACTS_OPERATION_CODEC = object(ContactsOperation::new,
			"contact", ContactsOperation::getContact, PUB_KEY_HEX_CODEC.nullable(),
			"remove", ContactsOperation::isRemove, StructuredCodecs.BOOLEAN_CODEC);

	@Nullable
	private final PubKey contact;
	private final boolean remove;

	private ContactsOperation(@Nullable PubKey contact, boolean remove) {
		this.contact = contact;
		this.remove = remove;
	}

	public static ContactsOperation add(PubKey friend) {
		return new ContactsOperation(friend, false);
	}

	public static ContactsOperation remove(PubKey friend) {
		return new ContactsOperation(friend, true);
	}

	public void apply(Set<PubKey> friends) {
		if (isEmpty()) {
			return;
		}

		if (remove) {
			friends.remove(contact);
		} else {
			friends.add(contact);
		}
	}

	public PubKey getContact() {
		assert contact != null;
		return contact;
	}

	public boolean isRemove() {
		return remove;
	}

	public ContactsOperation invert() {
		return new ContactsOperation(contact, !remove);
	}

	public boolean isEmpty() {
		return contact == null;
	}

	public boolean isInversionFor(ContactsOperation other) {
		return contact != null && other.contact != null &&
				contact.equals(other.contact) &&
				remove != other.remove;
	}
}
