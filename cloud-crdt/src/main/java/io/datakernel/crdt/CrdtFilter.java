package io.datakernel.crdt;

import java.util.function.Predicate;

@FunctionalInterface
public interface CrdtFilter<S> extends Predicate<S> {
}
