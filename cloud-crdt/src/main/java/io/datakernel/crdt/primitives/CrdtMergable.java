package io.datakernel.crdt.primitives;

public interface CrdtMergable<S extends CrdtMergable<S>> {

	S merge(S other);
}
