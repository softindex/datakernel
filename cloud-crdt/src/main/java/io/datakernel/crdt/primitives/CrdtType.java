package io.datakernel.crdt.primitives;

public interface CrdtType<S extends CrdtType<S>> extends CrdtMergable<S>, CrdtExtractable<S> {
}
