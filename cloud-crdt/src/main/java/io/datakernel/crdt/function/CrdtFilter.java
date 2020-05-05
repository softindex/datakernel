package io.datakernel.crdt.function;

import java.util.function.Predicate;

@FunctionalInterface
public interface CrdtFilter<S> extends Predicate<S> {
}
