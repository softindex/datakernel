package io.datakernel.ot;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.NotNull;

public interface GraphReducer<K, D, R> extends GraphVisitor<K, D> {
	@NotNull
	Promise<R> onFinish();
}
