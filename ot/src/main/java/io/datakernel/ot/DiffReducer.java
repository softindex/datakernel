package io.datakernel.ot;

import java.util.List;
import java.util.function.BiFunction;

public interface DiffReducer<A, D> extends BiFunction<A, List<D>, A> {}
