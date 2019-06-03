package io.global.ot.contactlist;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import static java.util.Collections.singletonList;

public final class ContactsOTSystem {
	private ContactsOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<ContactsOperation> createOTSystem() {
		return OTSystemImpl.<ContactsOperation>create()
				.withTransformFunction(ContactsOperation.class, ContactsOperation.class, ((left, right) -> TransformResult.of(right, left)))
				.withInvertFunction(ContactsOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(ContactsOperation.class, ContactsOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) return op2;
					if (op2.isEmpty()) return op1;
					if (op1.isInversionFor(op2)) {
						return ContactsOperation.EMPTY;
					}
					// changing name
					if (op1.getPubKey().equals(op2.getPubKey())) {
						return op2;
					}
					return null;
				})
				.withEmptyPredicate(ContactsOperation.class, ContactsOperation::isEmpty);
	}
}
