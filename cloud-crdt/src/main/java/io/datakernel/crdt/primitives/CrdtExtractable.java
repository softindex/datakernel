package io.datakernel.crdt.primitives;

import org.jetbrains.annotations.Nullable;

public interface CrdtExtractable<S extends CrdtExtractable<S>> {

	@Nullable
	S extract(long timestamp);
}
